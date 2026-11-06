import {
  THREAD_FILE_FORMAT_VERSION,
  deleteSavedThread,
  deleteSavedThreadRow,
  deleteThreadDependentRows,
  deleteThreadEventWindow,
  getAppSettings,
  getExperiments,
  getLatestThreadSequence,
  getSavedThread,
  getThread,
  hasRunningThreads,
  hideThreadForSaving,
  isThreadSavable,
  listSavedThreadsInStates,
  listThreadSaveCandidates,
  updateSavedThreadInState,
  upsertSavedThread,
} from "@bb/db";
import type { AppSettings } from "@bb/domain";
import {
  openThreadFile,
  removeSavedThreadFile,
  savedThreadFilePath,
  verifyThreadFileFrame,
  type OpenThreadFile,
  type ThreadFileFrameEntry,
} from "./thread-file.js";
import {
  isSavedThreadHeld,
  reconcileSavedThreads,
  removeOrphanedSavedThreadFiles,
  type SavedThreadDeps,
} from "./saved-thread-files.js";
import { ThreadFileExport } from "./thread-transfer.js";

export const THREAD_SAVING_SLICE_MS = 10;
const BUSY_REST_FACTOR = 9;
const IDLE_REST_FACTOR = 1;
const IDLE_POLL_MS = 60_000;
const ORPHAN_SWEEP_INTERVAL_MS = 10 * 60_000;
const DELETE_MAX_ROWS = 64;
const DELETE_MAX_BYTES = 512 * 1024;
const DELETE_DEPENDENT_ROWS = 128;
const CANDIDATE_SCAN_THREADS = 32;
const VACUUM_FREELIST_THRESHOLD_PAGES = 16_384;
const VACUUM_TARGET_MS = 6;
const VACUUM_MIN_PAGES = 8;
const VACUUM_MAX_PAGES = 256;
const STEP_COST_SMOOTHING = 0.2;
const DAY_MS = 86_400_000;

type StepOutcome = "worked" | "idle";

type CandidatePick =
  | { kind: "exhausted" }
  | { kind: "scanned" }
  | { kind: "thread"; requested: boolean; threadId: string };

interface ExportTask {
  exporter: ThreadFileExport;
  kind: "export";
  requested: boolean;
  threadId: string;
}

interface VerifyTask {
  frameIndex: number;
  frames: ThreadFileFrameEntry[];
  kind: "verify";
  packedBytes: number;
  rawBytes: number;
  requested: boolean;
  rows: number;
  threadId: string;
  verifiedRows: number;
}

interface DeleteTask {
  cursorSequence: number;
  eventsDone: boolean;
  kind: "delete";
  threadId: string;
}

type SavingTask = ExportTask | VerifyTask | DeleteTask;

export type ThreadSavingStepKind = "pick" | "vacuum" | SavingTask["kind"];

export function threadSaveCutoff(
  settings: Pick<AppSettings, "saveArchivedThreadsAfterDays">,
  now: number,
): number | null {
  return settings.saveArchivedThreadsAfterDays === "never"
    ? null
    : now - settings.saveArchivedThreadsAfterDays * DAY_MS;
}

export class ThreadSavingJob {
  private task: SavingTask | null = null;
  private requested: string[] = [];
  private candidates: string[] = [];
  private candidateCursor = 0;
  private scanPausedUntil = 0;
  private vacuumPages = 32;

  constructor(private readonly deps: SavedThreadDeps) {}

  requestSave(threadId: string): void {
    if (!this.requested.includes(threadId)) this.requested.push(threadId);
  }

  nextStepKind(): ThreadSavingStepKind {
    if (this.task !== null) return this.task.kind;
    return this.requested.length > 0 ||
      this.candidates.length > 0 ||
      Date.now() >= this.scanPausedUntil
      ? "pick"
      : "vacuum";
  }

  hasPendingWork(): boolean {
    return this.task !== null || this.requested.length > 0;
  }

  async step(): Promise<StepOutcome> {
    if (this.task === null) {
      const deleting = listSavedThreadsInStates(this.deps.db, ["deleting"])[0];
      if (deleting !== undefined) {
        this.task = {
          cursorSequence: deleting.cursorSequence,
          eventsDone: false,
          kind: "delete",
          threadId: deleting.threadId,
        };
        return "worked";
      }
    }
    if (!getExperiments(this.deps.db).savedThreads) {
      if (this.task?.kind === "delete") return this.deleteStep(this.task);
      await this.cancel();
      return "idle";
    }
    if (this.task === null) {
      const picked = this.pickCandidate();
      if (picked.kind === "scanned") return "worked";
      if (picked.kind === "thread" && (await this.startTask(picked))) return "worked";
      return this.vacuumStep();
    }
    switch (this.task.kind) {
      case "export":
        return this.exportStep(this.task);
      case "verify":
        return this.verifyStep(this.task);
      case "delete":
        return this.deleteStep(this.task);
    }
  }

  private async cancel(): Promise<void> {
    const task = this.task;
    this.requested = [];
    if (task === null || task.kind === "delete") return;
    await this.abandon(task, "The cold thread history experiment was turned off");
  }

  async stop(): Promise<void> {
    const task = this.task;
    this.task = null;
    if (task?.kind === "export") await task.exporter.abort();
  }

  private isStillSavable(
    threadId: string,
    requested: boolean,
    inProgress: boolean,
  ): boolean {
    if (isSavedThreadHeld(this.deps.db, threadId)) return false;
    if (this.deps.hub.listThreadTimelineTags(threadId).size > 0) return false;
    if (inProgress && getSavedThread(this.deps.db, threadId)?.state !== "saving") {
      return false;
    }
    const archivedBefore = requested
      ? null
      : threadSaveCutoff(getAppSettings(this.deps.db), Date.now());
    if (!requested && archivedBefore === null) return false;
    return isThreadSavable(this.deps.db, {
      archivedBefore,
      inProgress,
      threadId,
    });
  }

  private pickCandidate(): CandidatePick {
    while (this.requested.length > 0) {
      const threadId = this.requested.shift();
      if (threadId !== undefined && this.isStillSavable(threadId, true, false)) {
        return { kind: "thread", requested: true, threadId };
      }
    }
    while (this.candidates.length > 0) {
      const threadId = this.candidates.shift();
      if (threadId !== undefined && this.isStillSavable(threadId, false, false)) {
        return { kind: "thread", requested: false, threadId };
      }
    }
    const now = Date.now();
    if (now < this.scanPausedUntil) return { kind: "exhausted" };
    const archivedBefore = threadSaveCutoff(getAppSettings(this.deps.db), now);
    if (archivedBefore === null) return { kind: "exhausted" };
    const page = listThreadSaveCandidates(this.deps.db, {
      afterCursor: this.candidateCursor,
      archivedBefore,
      scanLimit: CANDIDATE_SCAN_THREADS,
    });
    this.candidateCursor = page.nextCursor ?? 0;
    this.candidates = page.threadIds;
    if (page.nextCursor === null) this.scanPausedUntil = now + IDLE_POLL_MS;
    return { kind: page.nextCursor === null ? "exhausted" : "scanned" };
  }

  private async startTask(pick: {
    requested: boolean;
    threadId: string;
  }): Promise<boolean> {
    const thread = getThread(this.deps.db, pick.threadId);
    if (thread === null) return false;
    const now = Date.now();
    const throughSequence = getLatestThreadSequence(this.deps.db, {
      threadId: pick.threadId,
    });
    const exporter = await ThreadFileExport.start(this.deps, {
      path: savedThreadFilePath(this.deps.config.dataDir, pick.threadId),
      threadId: pick.threadId,
      throughSequence,
    });
    if (exporter === null) return false;
    const previous = getSavedThread(this.deps.db, pick.threadId);
    upsertSavedThread(this.deps.db, {
      archivedAt: thread.archivedAt,
      cursorSequence: 0,
      formatVersion: THREAD_FILE_FORMAT_VERSION,
      packedBytes: 0,
      projectId: thread.projectId,
      providerId: thread.providerId,
      rawBytes: 0,
      restoredAt: previous?.restoredAt ?? null,
      rowCount: 0,
      savedAt: null,
      state: "saving",
      threadCreatedAt: thread.createdAt,
      threadId: pick.threadId,
      throughSequence,
      title: thread.title ?? thread.titleFallback,
      updatedAt: now,
    });
    this.task = {
      exporter,
      kind: "export",
      requested: pick.requested,
      threadId: pick.threadId,
    };
    return true;
  }

  private async abandon(task: ExportTask | VerifyTask, reason: string): Promise<void> {
    this.task = null;
    if (task.kind === "export") await task.exporter.abort();
    const saved = getSavedThread(this.deps.db, task.threadId);
    if (saved?.state === "saving") {
      if (saved.restoredAt === null) {
        deleteSavedThread(this.deps.db, task.threadId);
      } else {
        updateSavedThreadInState(this.deps.db, {
          expectedState: "saving",
          patch: { state: "restored", updatedAt: Date.now() },
          threadId: task.threadId,
        });
      }
      await removeSavedThreadFile(this.deps.config.dataDir, task.threadId);
    }
    this.deps.logger.debug({ threadId: task.threadId, reason }, "Thread save abandoned");
  }

  private async exportStep(task: ExportTask): Promise<StepOutcome> {
    if (!this.isStillSavable(task.threadId, task.requested, true)) {
      await this.abandon(task, "Thread is no longer eligible for saving");
      return "worked";
    }
    if (await task.exporter.step()) return "worked";
    const sealed = await task.exporter.seal();
    const file = await openThreadFile(
      savedThreadFilePath(this.deps.config.dataDir, task.threadId),
    );
    const frames = file.table.frames;
    await file.close();
    this.task = {
      frameIndex: 0,
      frames,
      kind: "verify",
      packedBytes: sealed.packedBytes,
      rawBytes: sealed.rawBytes,
      requested: task.requested,
      rows: sealed.rows,
      threadId: task.threadId,
      verifiedRows: 0,
    };
    return "worked";
  }

  private async verifyStep(task: VerifyTask): Promise<StepOutcome> {
    if (!this.isStillSavable(task.threadId, task.requested, true)) {
      await this.abandon(task, "Thread is no longer eligible for saving");
      return "worked";
    }
    const entry = task.frames[task.frameIndex];
    if (entry !== undefined) {
      let file: OpenThreadFile | null = null;
      try {
        file = await openThreadFile(
          savedThreadFilePath(this.deps.config.dataDir, task.threadId),
        );
        const rows = await verifyThreadFileFrame(file, entry);
        if (rows !== entry.rows) throw new Error("Thread file frame row count mismatch");
        task.verifiedRows += rows;
      } catch (error) {
        await this.abandon(
          task,
          error instanceof Error ? error.message : "Thread file verification failed",
        );
        return "worked";
      } finally {
        await file?.close();
      }
      task.frameIndex += 1;
      return "worked";
    }
    if (task.verifiedRows !== task.rows) {
      await this.abandon(task, "Thread file row count mismatch");
      return "worked";
    }
    const now = Date.now();
    const thread = getThread(this.deps.db, task.threadId);
    const hidden = this.deps.db.transaction(
      (tx) => {
        if (
          !updateSavedThreadInState(tx, {
            expectedState: "saving",
            patch: {
              packedBytes: task.packedBytes,
              rawBytes: task.rawBytes,
              rowCount: task.rows,
              state: "deleting",
              updatedAt: now,
            },
            threadId: task.threadId,
          })
        ) {
          return false;
        }
        hideThreadForSaving(tx, { now, threadId: task.threadId });
        return true;
      },
      { behavior: "immediate" },
    );
    if (!hidden) {
      await this.abandon(task, "Thread save was cancelled");
      return "worked";
    }
    if (thread !== null) {
      this.deps.hub.notifyThread(task.threadId, ["thread-deleted"], {
        projectId: thread.projectId,
      });
      this.deps.hub.notifyProject(thread.projectId, ["threads-changed"]);
    }
    this.task = {
      cursorSequence: 0,
      eventsDone: false,
      kind: "delete",
      threadId: task.threadId,
    };
    return "worked";
  }

  private deleteStep(task: DeleteTask): StepOutcome {
    const finished = this.deps.db.transaction(
      (tx) => {
        if (getSavedThread(tx, task.threadId)?.state !== "deleting") return null;
        const now = Date.now();
        if (!task.eventsDone) {
          const deleted = deleteThreadEventWindow(tx, {
            afterSequence: task.cursorSequence,
            maxBytes: DELETE_MAX_BYTES,
            maxRows: DELETE_MAX_ROWS,
            threadId: task.threadId,
          });
          if (deleted.lastSequence !== null) {
            task.cursorSequence = deleted.lastSequence;
            updateSavedThreadInState(tx, {
              expectedState: "deleting",
              patch: { cursorSequence: deleted.lastSequence, updatedAt: now },
              threadId: task.threadId,
            });
            return false;
          }
          task.eventsDone = true;
        }
        if (
          deleteThreadDependentRows(tx, {
            maxRows: DELETE_DEPENDENT_ROWS,
            threadId: task.threadId,
          }) > 0
        ) {
          return false;
        }
        deleteSavedThreadRow(tx, task.threadId);
        updateSavedThreadInState(tx, {
          expectedState: "deleting",
          patch: { savedAt: now, state: "saved", updatedAt: now },
          threadId: task.threadId,
        });
        return true;
      },
      { behavior: "immediate" },
    );
    if (finished === false) return "worked";
    this.task = null;
    if (finished === true) {
      this.deps.logger.info({ threadId: task.threadId }, "Thread saved to file");
    }
    return "worked";
  }

  private vacuumStep(): StepOutcome {
    const client = this.deps.db.$client;
    if (client.pragma("auto_vacuum", { simple: true }) !== 2) return "idle";
    const freelist = client.pragma("freelist_count", { simple: true });
    if (typeof freelist !== "number" || freelist < VACUUM_FREELIST_THRESHOLD_PAGES)
      return "idle";
    const startedAt = performance.now();
    client.pragma(`incremental_vacuum(${this.vacuumPages})`);
    const elapsed = performance.now() - startedAt;
    if (elapsed > VACUUM_TARGET_MS && this.vacuumPages > VACUUM_MIN_PAGES) {
      this.vacuumPages = Math.max(VACUUM_MIN_PAGES, Math.floor(this.vacuumPages / 2));
    } else if (elapsed < VACUUM_TARGET_MS / 3 && this.vacuumPages < VACUUM_MAX_PAGES) {
      this.vacuumPages = Math.min(VACUUM_MAX_PAGES, this.vacuumPages * 2);
    }
    return "worked";
  }
}

class StepCostEstimates {
  private readonly costs = new Map<ThreadSavingStepKind, number>();

  record(kind: ThreadSavingStepKind, ms: number): void {
    const previous = this.costs.get(kind);
    this.costs.set(
      kind,
      previous === undefined ? ms : previous + STEP_COST_SMOOTHING * (ms - previous),
    );
  }

  estimate(kind: ThreadSavingStepKind): number {
    return this.costs.get(kind) ?? 0;
  }
}

export interface ThreadSaving {
  requestSave(threadId: string): void;
  stop(): Promise<void>;
}

const savingByDb = new WeakMap<SavedThreadDeps["db"], ThreadSaving>();

export function getThreadSaving(db: SavedThreadDeps["db"]): ThreadSaving | null {
  return savingByDb.get(db) ?? null;
}

export function startThreadSaving(
  deps: SavedThreadDeps,
  options: { isFrozen(): boolean },
): ThreadSaving {
  const job = new ThreadSavingJob(deps);
  const stepCosts = new StepCostEstimates();
  let timer: ReturnType<typeof setTimeout> | null = null;
  let stopped = false;
  let running = false;
  let ready = false;
  let lastOrphanSweepAt = Date.now();

  const schedule = (delayMs: number) => {
    if (stopped) return;
    if (timer !== null) clearTimeout(timer);
    timer = setTimeout(threadSavingTick, delayMs);
    timer.unref();
  };

  async function threadSavingTick(): Promise<void> {
    timer = null;
    if (stopped || running || !ready) return;
    if (options.isFrozen()) {
      schedule(IDLE_POLL_MS);
      return;
    }
    running = true;
    const startedAt = performance.now();
    let outcome: StepOutcome = "idle";
    try {
      do {
        const kind = job.nextStepKind();
        const stepStartedAt = performance.now();
        outcome = await job.step();
        stepCosts.record(kind, performance.now() - stepStartedAt);
      } while (
        outcome === "worked" &&
        !stopped &&
        performance.now() - startedAt + stepCosts.estimate(job.nextStepKind()) <=
          THREAD_SAVING_SLICE_MS
      );
      if (
        outcome === "idle" &&
        !job.hasPendingWork() &&
        Date.now() - lastOrphanSweepAt >= ORPHAN_SWEEP_INTERVAL_MS
      ) {
        lastOrphanSweepAt = Date.now();
        await removeOrphanedSavedThreadFiles(deps);
      }
    } catch (error) {
      deps.logger.warn({ err: error }, "Thread saving step failed");
      outcome = "idle";
    } finally {
      running = false;
    }
    if (outcome === "idle" && !job.hasPendingWork()) {
      schedule(IDLE_POLL_MS);
      return;
    }
    const worked = performance.now() - startedAt;
    const busy = deps.hub.hasClients() || hasRunningThreads(deps.db);
    schedule(Math.max(1, worked * (busy ? BUSY_REST_FACTOR : IDLE_REST_FACTOR)));
  }

  void reconcileSavedThreads(deps)
    .catch((error: unknown) => {
      deps.logger.warn({ err: error }, "Saved thread reconciliation failed");
    })
    .finally(() => {
      ready = true;
      schedule(0);
    });

  const saving: ThreadSaving = {
    requestSave(threadId) {
      job.requestSave(threadId);
      if (ready) schedule(0);
    },
    async stop() {
      stopped = true;
      if (timer !== null) clearTimeout(timer);
      savingByDb.delete(deps.db);
      await job.stop();
    },
  };
  savingByDb.set(deps.db, saving);
  return saving;
}

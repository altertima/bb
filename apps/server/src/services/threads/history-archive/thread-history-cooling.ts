import {
  THREAD_HISTORY_ARCHIVE_FORMAT_VERSION,
  deleteThreadHistoryArchive,
  deleteThreadHistoryColdWindow,
  deleteThreadPruningCursorsForThread,
  getAppSettings,
  getLatestThreadSequence,
  getProject,
  getThread,
  getThreadHistoryArchive,
  hasRunningThreads,
  isThreadHistoryCoolable,
  listThreadHistoryCoolingCandidates,
  readThreadHistoryColdWindow,
  updateThreadHistoryArchiveInState,
  upsertThreadHistoryArchive,
  type ThreadHistoryCoolingCriteria,
} from "@bb/db";
import type { AppSettings } from "@bb/domain";
import type { AppDeps } from "../../../types.js";
import {
  THREAD_HISTORY_FRAME_RAW_BYTES,
  ThreadHistoryArchiveWriter,
  openThreadHistoryArchive,
  readThreadHistorySeekTable,
  removeThreadHistoryArchive,
  threadHistoryArchivePath,
  verifyThreadHistoryFrame,
  type ThreadHistoryFrameEntry,
} from "./archive-file.js";
import {
  ensureThreadHistoryHot,
  reconcileThreadHistoryArchives,
  removeOrphanedThreadHistoryFiles,
} from "./thread-history.js";

export type ThreadHistoryCoolingDeps = Pick<
  AppDeps,
  "config" | "db" | "hub" | "logger"
>;

export const THREAD_HISTORY_SLICE_MS = 10;
const BUSY_REST_FACTOR = 9;
const IDLE_REST_FACTOR = 1;
const IDLE_POLL_MS = 60_000;
const ORPHAN_SWEEP_INTERVAL_MS = 10 * 60_000;
const EXPORT_MAX_ROWS = 250;
const EXPORT_MAX_BYTES = 256 * 1024;
const DELETE_MAX_ROWS = 64;
const DELETE_MAX_BYTES = 512 * 1024;
const CANDIDATE_BATCH = 20;
const VACUUM_FREELIST_THRESHOLD_PAGES = 16_384;
const VACUUM_TARGET_MS = 6;
const VACUUM_MIN_PAGES = 8;
const VACUUM_MAX_PAGES = 256;
const DAY_MS = 86_400_000;

type StepOutcome = "worked" | "idle";

interface ExportTask {
  coldBeforeSequence: number;
  cursorSequence: number;
  kind: "export";
  threadId: string;
  writer: ThreadHistoryArchiveWriter;
}

interface VerifyTask {
  coldBeforeSequence: number;
  frameIndex: number;
  frames: ThreadHistoryFrameEntry[];
  kind: "verify";
  packedBytes: number;
  rawBytes: number;
  rows: number;
  threadId: string;
  verifiedRows: number;
}

interface DeleteTask {
  coldBeforeSequence: number;
  cursorSequence: number;
  kind: "delete";
  threadId: string;
}

type CoolingTask = ExportTask | VerifyTask | DeleteTask;

interface ForcedCooling {
  reject(error: Error): void;
  resolve(): void;
  threadId: string;
}

export function threadHistoryCoolingCriteria(
  settings: Pick<
    AppSettings,
    "coldHistoryAfterArchivedDays" | "coldHistoryAfterIdleDays"
  >,
  now: number,
): ThreadHistoryCoolingCriteria {
  return {
    archivedBefore:
      settings.coldHistoryAfterArchivedDays === "never"
        ? null
        : now - settings.coldHistoryAfterArchivedDays * DAY_MS,
    idleBefore:
      settings.coldHistoryAfterIdleDays === "never"
        ? null
        : now - settings.coldHistoryAfterIdleDays * DAY_MS,
  };
}

function forcedCriteria(now: number): ThreadHistoryCoolingCriteria {
  return { archivedBefore: now, idleBefore: now };
}

export class ThreadHistoryCoolingJob {
  private task: CoolingTask | null = null;
  private forced: ForcedCooling[] = [];
  private vacuumPages = 32;

  constructor(private readonly deps: ThreadHistoryCoolingDeps) {}

  requestCooling(threadId: string): Promise<void> {
    return new Promise((resolve, reject) => {
      this.forced.push({ reject, resolve, threadId });
    });
  }

  hasPendingWork(): boolean {
    return this.task !== null || this.forced.length > 0;
  }

  async step(): Promise<StepOutcome> {
    if (this.task === null) {
      const started = await this.startNextTask();
      if (started) return "worked";
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

  async stop(): Promise<void> {
    const task = this.task;
    this.task = null;
    if (task?.kind === "export") await task.writer.abort();
    for (const forced of this.forced.splice(0)) {
      forced.reject(new Error("Thread history cooling stopped"));
    }
  }

  private criteriaFor(threadId: string, now: number) {
    return this.forced.some((forced) => forced.threadId === threadId)
      ? forcedCriteria(now)
      : threadHistoryCoolingCriteria(getAppSettings(this.deps.db), now);
  }

  private isStillCoolable(threadId: string, inProgress = false): boolean {
    const now = Date.now();
    return (
      this.deps.hub.listThreadTimelineTags(threadId).size === 0 &&
      isThreadHistoryCoolable(this.deps.db, {
        ...this.criteriaFor(threadId, now),
        inProgress,
        threadId,
      })
    );
  }

  private settleForced(threadId: string, error: Error | null): void {
    const remaining: ForcedCooling[] = [];
    for (const forced of this.forced) {
      if (forced.threadId !== threadId) {
        remaining.push(forced);
      } else if (error === null) {
        forced.resolve();
      } else {
        forced.reject(error);
      }
    }
    this.forced = remaining;
  }

  private pickCandidate(): string | null {
    const now = Date.now();
    while (this.forced.length > 0) {
      const forced = this.forced[0];
      if (forced === undefined) break;
      if (this.isStillCoolable(forced.threadId)) return forced.threadId;
      this.settleForced(
        forced.threadId,
        new Error(
          "Thread history can't be archived now: the thread is busy, pinned, has pending work, or is open in a client",
        ),
      );
    }
    const candidates = listThreadHistoryCoolingCandidates(this.deps.db, {
      ...threadHistoryCoolingCriteria(getAppSettings(this.deps.db), now),
      limit: CANDIDATE_BATCH,
    });
    return (
      candidates.find(
        (threadId) =>
          this.deps.hub.listThreadTimelineTags(threadId).size === 0,
      ) ?? null
    );
  }

  private async startNextTask(): Promise<boolean> {
    const threadId = this.pickCandidate();
    if (threadId === null) return false;
    const thread = getThread(this.deps.db, threadId);
    if (thread === null) return false;
    const now = Date.now();
    const coldBeforeSequence = getLatestThreadSequence(this.deps.db, {
      threadId,
    });
    upsertThreadHistoryArchive(this.deps.db, {
      coldAt: null,
      coldBeforeSequence,
      cursorSequence: 0,
      formatVersion: THREAD_HISTORY_ARCHIVE_FORMAT_VERSION,
      packedBytes: 0,
      rawBytes: 0,
      restoredAt: getThreadHistoryArchive(this.deps.db, threadId)?.restoredAt ?? null,
      rowCount: 0,
      state: "exporting",
      threadId,
      updatedAt: now,
    });
    const project = getProject(this.deps.db, thread.projectId);
    const writer = await ThreadHistoryArchiveWriter.create({
      dataDir: this.deps.config.dataDir,
      header: {
        archivedAt: thread.archivedAt,
        projectId: thread.projectId,
        projectName: project?.name ?? null,
        threadId,
        title: thread.title ?? thread.titleFallback,
      },
    });
    this.task = {
      coldBeforeSequence,
      cursorSequence: 0,
      kind: "export",
      threadId,
      writer,
    };
    return true;
  }

  private async abandon(task: CoolingTask, reason: string): Promise<void> {
    this.task = null;
    if (task.kind === "export") await task.writer.abort();
    const archive = getThreadHistoryArchive(this.deps.db, task.threadId);
    if (archive?.state === "exporting") {
      deleteThreadHistoryArchive(this.deps.db, task.threadId);
      await removeThreadHistoryArchive(this.deps.config.dataDir, task.threadId);
    }
    this.deps.logger.debug(
      { threadId: task.threadId, reason },
      "Thread history cooling abandoned",
    );
    this.settleForced(task.threadId, new Error(reason));
  }

  private archiveStillExporting(threadId: string): boolean {
    return getThreadHistoryArchive(this.deps.db, threadId)?.state === "exporting";
  }

  private async exportStep(task: ExportTask): Promise<StepOutcome> {
    if (!this.archiveStillExporting(task.threadId)) {
      await this.abandon(task, "Thread history archive changed during export");
      return "worked";
    }
    if (!this.isStillCoolable(task.threadId, true)) {
      await this.abandon(task, "Thread is no longer eligible for history archiving");
      return "worked";
    }
    const window = readThreadHistoryColdWindow(this.deps.db, {
      afterSequence: task.cursorSequence,
      beforeSequence: task.coldBeforeSequence,
      maxBytes: EXPORT_MAX_BYTES,
      maxRows: EXPORT_MAX_ROWS,
      threadId: task.threadId,
    });
    if (window.lastSequence === null) {
      const sealed = await task.writer.seal();
      const table = await readThreadHistorySeekTable(
        threadHistoryArchivePath(this.deps.config.dataDir, task.threadId),
      );
      this.task = {
        coldBeforeSequence: task.coldBeforeSequence,
        frameIndex: 0,
        frames: table.frames,
        kind: "verify",
        packedBytes: sealed.packedBytes,
        rawBytes: sealed.rawBytes,
        rows: sealed.rows,
        threadId: task.threadId,
        verifiedRows: 0,
      };
      return "worked";
    }
    task.writer.addWindow(window.rows, window.retained);
    task.cursorSequence = window.lastSequence;
    if (task.writer.pendingRawBytes >= THREAD_HISTORY_FRAME_RAW_BYTES) {
      await task.writer.flushFrame();
    }
    return "worked";
  }

  private async verifyStep(task: VerifyTask): Promise<StepOutcome> {
    if (!this.archiveStillExporting(task.threadId)) {
      await this.abandon(task, "Thread history archive changed during verification");
      return "worked";
    }
    const entry = task.frames[task.frameIndex];
    if (entry !== undefined) {
      const file = await openThreadHistoryArchive(
        threadHistoryArchivePath(this.deps.config.dataDir, task.threadId),
      );
      try {
        const rows = await verifyThreadHistoryFrame(file.handle, entry);
        if (rows !== entry.rows) {
          throw new Error("Thread history archive frame row count mismatch");
        }
        task.verifiedRows += rows;
      } catch (error) {
        await this.abandon(
          task,
          error instanceof Error ? error.message : "Archive verification failed",
        );
        return "worked";
      } finally {
        await file.close();
      }
      task.frameIndex += 1;
      return "worked";
    }
    if (task.verifiedRows !== task.rows) {
      await this.abandon(task, "Thread history archive row count mismatch");
      return "worked";
    }
    updateThreadHistoryArchiveInState(this.deps.db, {
      expectedState: "exporting",
      patch: {
        packedBytes: task.packedBytes,
        rawBytes: task.rawBytes,
        rowCount: task.rows,
        state: "cooling",
        updatedAt: Date.now(),
      },
      threadId: task.threadId,
    });
    this.task = {
      coldBeforeSequence: task.coldBeforeSequence,
      cursorSequence: 0,
      kind: "delete",
      threadId: task.threadId,
    };
    return "worked";
  }

  private async deleteStep(task: DeleteTask): Promise<StepOutcome> {
    if (this.deps.hub.listThreadTimelineTags(task.threadId).size > 0) {
      this.task = null;
      await ensureThreadHistoryHot(this.deps, task.threadId);
      this.settleForced(
        task.threadId,
        new Error("Thread was opened while its history was being archived"),
      );
      return "worked";
    }
    const result = this.deps.db.transaction(
      (tx) => {
        const archive = getThreadHistoryArchive(tx, task.threadId);
        if (archive?.state !== "cooling") return null;
        const deleted = deleteThreadHistoryColdWindow(tx, {
          afterSequence: task.cursorSequence,
          beforeSequence: task.coldBeforeSequence,
          maxBytes: DELETE_MAX_BYTES,
          maxRows: DELETE_MAX_ROWS,
          threadId: task.threadId,
        });
        const now = Date.now();
        if (deleted.lastSequence === null) {
          deleteThreadPruningCursorsForThread(tx, task.threadId);
          updateThreadHistoryArchiveInState(tx, {
            expectedState: "cooling",
            patch: { coldAt: now, state: "cold", updatedAt: now },
            threadId: task.threadId,
          });
        } else {
          updateThreadHistoryArchiveInState(tx, {
            expectedState: "cooling",
            patch: { cursorSequence: deleted.lastSequence, updatedAt: now },
            threadId: task.threadId,
          });
        }
        return deleted;
      },
      { behavior: "immediate" },
    );
    if (result === null) {
      this.task = null;
      this.settleForced(
        task.threadId,
        new Error("Thread history was restored while it was being archived"),
      );
      return "worked";
    }
    if (result.lastSequence === null) {
      this.task = null;
      this.deps.logger.info(
        { threadId: task.threadId },
        "Thread history moved to archive",
      );
      this.settleForced(task.threadId, null);
      return "worked";
    }
    task.cursorSequence = result.lastSequence;
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

export interface ThreadHistoryMaintenance {
  coolNow(threadId: string): Promise<void>;
  stop(): Promise<void>;
}

const maintenanceByDb = new WeakMap<
  ThreadHistoryCoolingDeps["db"],
  ThreadHistoryMaintenance
>();

export function getThreadHistoryMaintenance(
  db: ThreadHistoryCoolingDeps["db"],
): ThreadHistoryMaintenance | null {
  return maintenanceByDb.get(db) ?? null;
}

export function startThreadHistoryMaintenance(
  deps: ThreadHistoryCoolingDeps,
  options: { isFrozen(): boolean },
): ThreadHistoryMaintenance {
  const job = new ThreadHistoryCoolingJob(deps);
  let timer: ReturnType<typeof setTimeout> | null = null;
  let stopped = false;
  let running = false;
  let ready = false;
  let lastOrphanSweepAt = Date.now();

  const schedule = (delayMs: number) => {
    if (stopped) return;
    if (timer !== null) clearTimeout(timer);
    timer = setTimeout(threadHistoryMaintenanceTick, delayMs);
    timer.unref();
  };

  async function threadHistoryMaintenanceTick(): Promise<void> {
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
        outcome = await job.step();
      } while (
        outcome === "worked" &&
        !stopped &&
        performance.now() - startedAt < THREAD_HISTORY_SLICE_MS
      );
      if (
        outcome === "idle" &&
        !job.hasPendingWork() &&
        Date.now() - lastOrphanSweepAt >= ORPHAN_SWEEP_INTERVAL_MS
      ) {
        lastOrphanSweepAt = Date.now();
        await removeOrphanedThreadHistoryFiles(deps);
      }
    } catch (error) {
      deps.logger.warn({ err: error }, "Thread history maintenance step failed");
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
    schedule(
      Math.max(1, worked * (busy ? BUSY_REST_FACTOR : IDLE_REST_FACTOR)),
    );
  }

  void reconcileThreadHistoryArchives(deps)
    .catch((error: unknown) => {
      deps.logger.warn(
        { err: error },
        "Thread history archive reconciliation failed",
      );
    })
    .finally(() => {
      ready = true;
      schedule(job.hasPendingWork() ? 0 : IDLE_POLL_MS);
    });

  const maintenance: ThreadHistoryMaintenance = {
    coolNow(threadId) {
      const cooled = job.requestCooling(threadId);
      if (ready) schedule(0);
      return cooled;
    },
    async stop() {
      stopped = true;
      if (timer !== null) clearTimeout(timer);
      maintenanceByDb.delete(deps.db);
      await job.stop();
    },
  };
  maintenanceByDb.set(deps.db, maintenance);
  return maintenance;
}

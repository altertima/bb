import { rm } from "node:fs/promises";
import {
  deleteThreadHistoryArchive,
  getThreadHistoryArchive,
  insertThreadHistoryRows,
  listThreadHistoryArchiveThreadIds,
  listThreadHistoryArchivesInStates,
  updateThreadHistoryArchiveInState,
  type DbConnection,
  type ThreadHistoryArchive,
} from "@bb/db";
import type { AppDeps } from "../../../types.js";
import { requirePublicThread } from "../../lib/entity-lookup.js";
import {
  listThreadHistoryArchiveFiles,
  openThreadHistoryArchive,
  readThreadHistoryFrame,
  removeThreadHistoryArchive,
  threadHistoryArchiveExists,
  threadHistoryArchivePath,
} from "./archive-file.js";

export type ThreadHistoryDeps = Pick<AppDeps, "config" | "db" | "logger">;

export type ThreadHistoryStorage = "hot" | "cold";

export interface ThreadHistoryStatus {
  archivedRows: number;
  coldAt: number | null;
  packedBytes: number;
  storage: ThreadHistoryStorage;
  threadId: string;
}

const RESTORE_WINDOW_ROWS = 256;

const restoresByDb = new WeakMap<DbConnection, Map<string, Promise<void>>>();

function restoresFor(db: DbConnection): Map<string, Promise<void>> {
  let restores = restoresByDb.get(db);
  if (restores === undefined) {
    restores = new Map();
    restoresByDb.set(db, restores);
  }
  return restores;
}

function needsRestore(archive: ThreadHistoryArchive | null): boolean {
  return (
    archive !== null &&
    (archive.state === "cooling" ||
      archive.state === "cold" ||
      archive.state === "restoring")
  );
}

function yieldToEventLoop(): Promise<void> {
  return new Promise((resolve) => setImmediate(resolve));
}

export function threadHistoryStatus(
  deps: Pick<AppDeps, "db">,
  threadId: string,
): ThreadHistoryStatus {
  const archive = getThreadHistoryArchive(deps.db, threadId);
  const cold = needsRestore(archive);
  return {
    archivedRows: cold && archive !== null ? archive.rowCount : 0,
    coldAt: cold && archive !== null ? archive.coldAt : null,
    packedBytes: cold && archive !== null ? archive.packedBytes : 0,
    storage: cold ? "cold" : "hot",
    threadId,
  };
}

export async function ensureThreadHistoryHot(
  deps: ThreadHistoryDeps,
  threadId: string,
): Promise<void> {
  if (!needsRestore(getThreadHistoryArchive(deps.db, threadId))) return;
  const restores = restoresFor(deps.db);
  let restore = restores.get(threadId);
  if (restore === undefined) {
    restore = restoreThreadHistory(deps, threadId).finally(() => {
      restores.delete(threadId);
    });
    restores.set(threadId, restore);
  }
  await restore;
}

function markRestoring(db: DbConnection, archive: ThreadHistoryArchive): void {
  if (archive.state === "restoring") return;
  updateThreadHistoryArchiveInState(db, {
    expectedState: archive.state,
    patch: { state: "restoring", updatedAt: Date.now() },
    threadId: archive.threadId,
  });
}

function markHot(db: DbConnection, threadId: string, restoredAt: number): void {
  updateThreadHistoryArchiveInState(db, {
    expectedState: "restoring",
    patch: {
      coldAt: null,
      cursorSequence: 0,
      packedBytes: 0,
      rawBytes: 0,
      restoredAt,
      rowCount: 0,
      state: "hot",
      updatedAt: restoredAt,
    },
    threadId,
  });
}

async function restoreThreadHistory(
  deps: ThreadHistoryDeps,
  threadId: string,
): Promise<void> {
  const archive = getThreadHistoryArchive(deps.db, threadId);
  if (archive === null || !needsRestore(archive)) return;
  const startedAt = performance.now();
  markRestoring(deps.db, archive);
  const path = threadHistoryArchivePath(deps.config.dataDir, threadId);
  let inserted = 0;
  if (!(await threadHistoryArchiveExists(deps.config.dataDir, threadId))) {
    if (archive.rowCount > 0) {
      deps.logger.error(
        { threadId, rows: archive.rowCount },
        "Thread history archive file is missing; its archived events cannot be restored",
      );
    }
  } else {
    const file = await openThreadHistoryArchive(path);
    try {
      for (const entry of file.table.frames) {
        const frame = await readThreadHistoryFrame(file.handle, entry);
        for (
          let offset = 0;
          offset < frame.rows.length;
          offset += RESTORE_WINDOW_ROWS
        ) {
          const rows = frame.rows.slice(offset, offset + RESTORE_WINDOW_ROWS);
          const eventIds = new Set(rows.map((row) => row.id));
          const now = Date.now();
          inserted += deps.db.transaction(
            (tx) =>
              insertThreadHistoryRows(tx, {
                now,
                retained: frame.retained.filter((output) =>
                  eventIds.has(output.eventId),
                ),
                rows,
              }),
            { behavior: "immediate" },
          );
          await yieldToEventLoop();
        }
      }
    } finally {
      await file.close();
    }
  }
  markHot(deps.db, threadId, Date.now());
  await removeThreadHistoryArchive(deps.config.dataDir, threadId);
  deps.logger.info(
    {
      threadId,
      restoredRows: inserted,
      durationMs: Math.round(performance.now() - startedAt),
    },
    "Thread history restored from archive",
  );
}

export async function dropThreadHistoryArchive(
  deps: ThreadHistoryDeps,
  threadId: string,
): Promise<void> {
  await ensureThreadHistoryHot(deps, threadId);
  deleteThreadHistoryArchive(deps.db, threadId);
  await removeThreadHistoryArchive(deps.config.dataDir, threadId);
}

export async function removeOrphanedThreadHistoryFiles(
  deps: ThreadHistoryDeps,
): Promise<number> {
  const known = listThreadHistoryArchiveThreadIds(deps.db);
  let removed = 0;
  for (const file of await listThreadHistoryArchiveFiles(deps.config.dataDir)) {
    const archive = known.has(file.threadId)
      ? getThreadHistoryArchive(deps.db, file.threadId)
      : null;
    const keep = file.temporary
      ? archive?.state === "exporting"
      : needsRestore(archive) || archive?.state === "exporting";
    if (!keep) {
      await rm(file.path, { force: true });
      removed += 1;
    }
  }
  return removed;
}

export async function reconcileThreadHistoryArchives(
  deps: ThreadHistoryDeps,
): Promise<void> {
  for (const archive of listThreadHistoryArchivesInStates(deps.db, [
    "exporting",
  ])) {
    deleteThreadHistoryArchive(deps.db, archive.threadId);
    await removeThreadHistoryArchive(deps.config.dataDir, archive.threadId);
  }
  await removeOrphanedThreadHistoryFiles(deps);
}

export async function requireHotPublicThread(
  deps: ThreadHistoryDeps,
  threadId: string,
): Promise<ReturnType<typeof requirePublicThread>> {
  const thread = requirePublicThread(deps.db, threadId);
  await ensureThreadHistoryHot(deps, thread.id);
  return thread;
}

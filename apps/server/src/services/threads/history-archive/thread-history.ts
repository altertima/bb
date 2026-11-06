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
import { ApiError } from "../../../errors.js";
import type { AppDeps } from "../../../types.js";
import { requirePublicThread } from "../../lib/entity-lookup.js";
import {
  ThreadFileError,
  listThreadHistoryArchiveFiles,
  openThreadFile,
  readThreadFileFrame,
  removeThreadHistoryArchive,
  threadHistoryArchiveExists,
  threadHistoryArchivePath,
  type OpenThreadFile,
} from "./thread-file.js";

export type ThreadHistoryDeps = Pick<AppDeps, "config" | "db" | "logger">;

const INSERT_WINDOW_ROWS = 256;

const heldByDb = new WeakMap<DbConnection, Map<string, number>>();

export function holdThreadHistory(db: DbConnection, threadId: string): () => void {
  let held = heldByDb.get(db);
  if (held === undefined) {
    held = new Map();
    heldByDb.set(db, held);
  }
  const holds = held;
  holds.set(threadId, (holds.get(threadId) ?? 0) + 1);
  let released = false;
  return () => {
    if (released) return;
    released = true;
    const remaining = (holds.get(threadId) ?? 1) - 1;
    if (remaining === 0) holds.delete(threadId);
    else holds.set(threadId, remaining);
  };
}

export function isThreadHistoryHeld(db: DbConnection, threadId: string): boolean {
  return heldByDb.get(db)?.has(threadId) ?? false;
}

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

export function isThreadHistoryCold(db: DbConnection, threadId: string): boolean {
  return needsRestore(getThreadHistoryArchive(db, threadId));
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

export async function insertThreadFileRows(
  deps: ThreadHistoryDeps,
  file: OpenThreadFile,
): Promise<number> {
  let inserted = 0;
  for (const entry of file.table.frames) {
    const frame = await readThreadFileFrame(file, entry);
    for (let offset = 0; offset < frame.rows.length; offset += INSERT_WINDOW_ROWS) {
      const rows = frame.rows.slice(offset, offset + INSERT_WINDOW_ROWS);
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
  return inserted;
}

async function openArchiveForRestore(
  deps: ThreadHistoryDeps,
  archive: ThreadHistoryArchive,
): Promise<OpenThreadFile | null> {
  if (!(await threadHistoryArchiveExists(deps.config.dataDir, archive.threadId))) {
    if (archive.rowCount > 0) {
      deps.logger.error(
        { threadId: archive.threadId, rows: archive.rowCount },
        "Thread history archive file is missing; its archived events cannot be restored",
      );
    }
    return null;
  }
  try {
    const file = await openThreadFile(
      threadHistoryArchivePath(deps.config.dataDir, archive.threadId),
    );
    if (file.header.threadId !== archive.threadId) {
      await file.close();
      throw new ThreadFileError(
        `Thread history archive for ${archive.threadId} belongs to thread ${file.header.threadId}`,
      );
    }
    return file;
  } catch (error) {
    if (!(error instanceof ThreadFileError)) throw error;
    deps.logger.error(
      { threadId: archive.threadId, err: error },
      "Thread history archive cannot be read; leaving the thread's history archived",
    );
    throw new ApiError(409, "thread_history_unreadable", error.message);
  }
}

async function restoreThreadHistory(
  deps: ThreadHistoryDeps,
  threadId: string,
): Promise<void> {
  const archive = getThreadHistoryArchive(deps.db, threadId);
  if (archive === null || !needsRestore(archive)) return;
  const startedAt = performance.now();
  const file = await openArchiveForRestore(deps, archive);
  markRestoring(deps.db, archive);
  let inserted = 0;
  if (file !== null) {
    try {
      inserted = await insertThreadFileRows(deps, file);
    } catch (error) {
      if (!(error instanceof ThreadFileError)) throw error;
      deps.logger.error(
        { threadId, err: error },
        "Thread history archive is damaged; leaving the thread's history archived",
      );
      throw new ApiError(409, "thread_history_unreadable", error.message);
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

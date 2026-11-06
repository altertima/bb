import {
  deleteThreadHistoryArchive,
  getProject,
  getThread,
  getThreadHistoryArchive,
  readThreadEventWindow,
  type DbConnection,
  type ThreadHistoryEventRow,
  type ThreadHistoryRetainedOutputRow,
} from "@bb/db";
import { ApiError } from "../../../errors.js";
import {
  THREAD_FILE_FRAME_RAW_BYTES,
  ThreadFileError,
  ThreadFileWriter,
  openThreadFile,
  readThreadFileFrame,
  threadHistoryArchivePath,
  type OpenThreadFile,
  type ThreadFileHeader,
} from "./thread-file.js";
import {
  ensureThreadHistoryHot,
  holdThreadHistory,
  insertThreadFileRows,
  isThreadHistoryCold,
  type ThreadHistoryDeps,
} from "./thread-history.js";

const THREAD_FILE_WINDOW_ROWS = 128;
const THREAD_FILE_WINDOW_BYTES = 128 * 1024;

export interface ThreadFileSummary {
  packedBytes: number;
  rawBytes: number;
  rows: number;
}

export interface ThreadFileImportResult {
  insertedRows: number;
  rows: number;
  threadId: string;
}

function threadFileHeader(
  db: DbConnection,
  threadId: string,
  exportedAt: number,
): ThreadFileHeader | null {
  const thread = getThread(db, threadId);
  if (thread === null) return null;
  return {
    archivedAt: thread.archivedAt,
    createdAt: thread.createdAt,
    exportedAt,
    projectId: thread.projectId,
    projectName: getProject(db, thread.projectId)?.name ?? null,
    providerId: thread.providerId,
    threadId,
    title: thread.title ?? thread.titleFallback,
  };
}

export class ThreadFileExport {
  private constructor(
    private readonly db: DbConnection,
    private readonly writer: ThreadFileWriter,
    readonly threadId: string,
    private afterSequence: number,
    private readonly throughSequence: number | null,
  ) {}

  static async start(
    db: DbConnection,
    args: {
      afterSequence: number;
      path: string;
      threadId: string;
      throughSequence: number | null;
    },
  ): Promise<ThreadFileExport | null> {
    const header = threadFileHeader(db, args.threadId, Date.now());
    if (header === null) return null;
    const writer = await ThreadFileWriter.create({ header, path: args.path });
    return new ThreadFileExport(
      db,
      writer,
      args.threadId,
      args.afterSequence,
      args.throughSequence,
    );
  }

  async add(
    rows: readonly ThreadHistoryEventRow[],
    retained: readonly ThreadHistoryRetainedOutputRow[],
  ): Promise<void> {
    this.writer.addWindow(rows, retained);
    if (this.writer.pendingRawBytes >= THREAD_FILE_FRAME_RAW_BYTES) {
      await this.writer.flushFrameInBackground();
    }
  }

  async step(): Promise<boolean> {
    const window = readThreadEventWindow(this.db, {
      afterSequence: this.afterSequence,
      maxBytes: THREAD_FILE_WINDOW_BYTES,
      maxRows: THREAD_FILE_WINDOW_ROWS,
      threadId: this.threadId,
      throughSequence: this.throughSequence,
    });
    if (window.lastSequence === null) return false;
    this.afterSequence = window.lastSequence;
    await this.add(window.rows, window.retained);
    return true;
  }

  seal(): Promise<ThreadFileSummary> {
    return this.writer.seal();
  }

  abort(): Promise<void> {
    return this.writer.abort();
  }
}

function yieldToEventLoop(): Promise<void> {
  return new Promise((resolve) => setImmediate(resolve));
}

async function openColdHistory(
  deps: ThreadHistoryDeps,
  threadId: string,
): Promise<OpenThreadFile | null> {
  if (!isThreadHistoryCold(deps.db, threadId)) return null;
  try {
    return await openThreadFile(
      threadHistoryArchivePath(deps.config.dataDir, threadId),
    );
  } catch (error) {
    if (error instanceof ThreadFileError) {
      throw new ApiError(409, "thread_history_unreadable", error.message);
    }
    if (isThreadHistoryCold(deps.db, threadId)) throw error;
    return null;
  }
}

export async function exportThreadFile(
  deps: ThreadHistoryDeps,
  args: { path: string; threadId: string },
): Promise<ThreadFileSummary> {
  const cold = await openColdHistory(deps, args.threadId);
  try {
    const exporter = await ThreadFileExport.start(deps.db, {
      afterSequence: cold?.table.lastSequence ?? 0,
      path: args.path,
      threadId: args.threadId,
      throughSequence: null,
    });
    if (exporter === null) {
      throw new ApiError(404, "thread_not_found", "Thread not found");
    }
    try {
      if (cold !== null) {
        for (const entry of cold.table.frames) {
          const frame = await readThreadFileFrame(cold, entry);
          await exporter.add(frame.rows, frame.retained);
        }
      }
      while (await exporter.step()) await yieldToEventLoop();
      return await exporter.seal();
    } catch (error) {
      await exporter.abort();
      if (error instanceof ThreadFileError) {
        throw new ApiError(409, "thread_history_unreadable", error.message);
      }
      throw error;
    }
  } finally {
    await cold?.close();
  }
}

export async function importThreadFile(
  deps: ThreadHistoryDeps,
  path: string,
): Promise<ThreadFileImportResult> {
  let file: OpenThreadFile;
  try {
    file = await openThreadFile(path);
    try {
      for (const entry of file.table.frames) {
        await readThreadFileFrame(file, entry);
        await yieldToEventLoop();
      }
    } catch (error) {
      await file.close();
      throw error;
    }
  } catch (error) {
    if (error instanceof ThreadFileError) {
      throw new ApiError(400, "invalid_thread_file", error.message);
    }
    throw error;
  }
  try {
    const { threadId } = file.header;
    const thread = getThread(deps.db, threadId);
    const project = thread === null ? null : getProject(deps.db, thread.projectId);
    if (
      thread === null ||
      thread.deletedAt !== null ||
      project === null ||
      project.deletedAt !== null
    ) {
      throw new ApiError(
        404,
        "thread_not_found",
        `Thread ${threadId} isn't on this server. Importing a thread from another server isn't supported yet.`,
      );
    }
    const release = holdThreadHistory(deps.db, threadId);
    try {
      if (getThreadHistoryArchive(deps.db, threadId)?.state === "exporting") {
        deleteThreadHistoryArchive(deps.db, threadId);
      }
      await ensureThreadHistoryHot(deps, threadId);
      let insertedRows: number;
      try {
        insertedRows = await insertThreadFileRows(deps, file);
      } catch (error) {
        if (error instanceof ThreadFileError) {
          throw new ApiError(400, "invalid_thread_file", error.message);
        }
        throw error;
      }
      deps.logger.info(
        { threadId, rows: file.table.rows, insertedRows },
        "Thread file imported",
      );
      return { insertedRows, rows: file.table.rows, threadId };
    } finally {
      release();
    }
  } finally {
    await file.close();
  }
}

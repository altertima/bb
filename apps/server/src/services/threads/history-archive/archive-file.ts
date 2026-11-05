import { createHash } from "node:crypto";
import { mkdir, open, readdir, rename, rm, stat } from "node:fs/promises";
import type { FileHandle } from "node:fs/promises";
import { join } from "node:path";
import { promisify } from "node:util";
import zlib from "node:zlib";
import { z } from "zod";
import {
  THREAD_HISTORY_ARCHIVE_FORMAT_VERSION,
  type ThreadHistoryEventRow,
  type ThreadHistoryRetainedOutputRow,
} from "@bb/db";

const zstdCompress = promisify(zlib.zstdCompress);
const zstdDecompress = promisify(zlib.zstdDecompress);

const ZSTD_LEVEL = 9;
const SKIPPABLE_FRAME_MAGIC = 0x184d2a5e;
const SEEK_TRAILER_MAGIC = "BBHSEEK1";
const SEEK_TRAILER_BYTES = 4 + SEEK_TRAILER_MAGIC.length;
const ARCHIVE_EXTENSION = ".jsonl.zst";
const TEMP_SUFFIX = ".tmp";

export const THREAD_HISTORY_FRAME_RAW_BYTES = 1024 * 1024;

export interface ThreadHistoryArchiveHeader {
  archivedAt: number | null;
  projectId: string;
  projectName: string | null;
  threadId: string;
  title: string | null;
}

export interface ThreadHistoryFrameEntry {
  compressedBytes: number;
  firstSequence: number;
  lastSequence: number;
  offset: number;
  rawBytes: number;
  rows: number;
  sha256: string;
}

export interface ThreadHistorySeekTable {
  formatVersion: number;
  frames: ThreadHistoryFrameEntry[];
  rawBytes: number;
  rows: number;
  threadId: string;
}

export interface ThreadHistoryFrameContents {
  retained: ThreadHistoryRetainedOutputRow[];
  rows: ThreadHistoryEventRow[];
}

const frameEntrySchema = z.object({
  compressedBytes: z.number().int().nonnegative(),
  firstSequence: z.number().int(),
  lastSequence: z.number().int(),
  offset: z.number().int().nonnegative(),
  rawBytes: z.number().int().nonnegative(),
  rows: z.number().int().nonnegative(),
  sha256: z.string().length(64),
});

const seekTableSchema = z.object({
  formatVersion: z.number().int(),
  frames: z.array(frameEntrySchema),
  rawBytes: z.number().int().nonnegative(),
  rows: z.number().int().nonnegative(),
  threadId: z.string().min(1),
});

const eventLineBaseSchema = z.object({
  kind: z.literal("event"),
  createdAt: z.number().int(),
  environmentId: z.string().nullable(),
  id: z.string().min(1),
  itemId: z.string().nullable(),
  itemKind: z.string().nullable(),
  parentToolCallId: z.string().nullable(),
  providerThreadId: z.string().nullable(),
  scopeKind: z.string().min(1),
  sequence: z.number().int(),
  threadId: z.string().min(1),
  turnId: z.string().nullable(),
  type: z.string().min(1),
});

const eventLineSchema = z.union([
  eventLineBaseSchema.extend({ data: z.record(z.string(), z.unknown()) }),
  eventLineBaseSchema.extend({ dataText: z.string() }),
]);

const retainedLineSchema = z.object({
  kind: z.literal("retained"),
  eventId: z.string().min(1),
  expiresAt: z.number().int(),
  outputPath: z.string().min(1),
  value: z.string(),
});

export function threadHistoryArchiveDir(dataDir: string): string {
  return join(dataDir, "archive", "threads");
}

export function threadHistoryArchivePath(
  dataDir: string,
  threadId: string,
): string {
  return join(threadHistoryArchiveDir(dataDir), `${threadId}${ARCHIVE_EXTENSION}`);
}

function eventLine(row: ThreadHistoryEventRow): string {
  const { data, ...columns } = row;
  let nested: unknown;
  try {
    nested = JSON.parse(data);
  } catch {
    nested = undefined;
  }
  const isObject =
    typeof nested === "object" && nested !== null && !Array.isArray(nested);
  if (isObject && JSON.stringify(nested) === data) {
    return JSON.stringify({ kind: "event", ...columns, data: nested });
  }
  return JSON.stringify({ kind: "event", ...columns, dataText: data });
}

function retainedLine(row: ThreadHistoryRetainedOutputRow): string {
  return JSON.stringify({ kind: "retained", ...row });
}

export class ThreadHistoryArchiveWriter {
  private frames: ThreadHistoryFrameEntry[] = [];
  private pendingLines: string[] = [];
  private pendingBytes = 0;
  private pendingRows = 0;
  private pendingFirst: number | null = null;
  private pendingLast: number | null = null;
  private offset = 0;
  private totalRows = 0;
  private totalRawBytes = 0;

  private constructor(
    private readonly handle: FileHandle,
    private readonly tempPath: string,
    private readonly finalPath: string,
    private readonly threadId: string,
  ) {}

  static async create(args: {
    dataDir: string;
    header: ThreadHistoryArchiveHeader;
  }): Promise<ThreadHistoryArchiveWriter> {
    await mkdir(threadHistoryArchiveDir(args.dataDir), { recursive: true });
    const finalPath = threadHistoryArchivePath(
      args.dataDir,
      args.header.threadId,
    );
    const tempPath = `${finalPath}${TEMP_SUFFIX}`;
    const handle = await open(tempPath, "w");
    const writer = new ThreadHistoryArchiveWriter(
      handle,
      tempPath,
      finalPath,
      args.header.threadId,
    );
    const headerFrame = await zstdCompress(
      Buffer.from(
        `${JSON.stringify({
          kind: "header",
          bbArchive: 1,
          formatVersion: THREAD_HISTORY_ARCHIVE_FORMAT_VERSION,
          ...args.header,
        })}\n`,
      ),
      { params: { [zlib.constants.ZSTD_c_compressionLevel]: ZSTD_LEVEL } },
    );
    await writer.write(headerFrame);
    return writer;
  }

  get pendingRawBytes(): number {
    return this.pendingBytes;
  }

  get rowCount(): number {
    return this.totalRows + this.pendingRows;
  }

  addWindow(
    rows: readonly ThreadHistoryEventRow[],
    retained: readonly ThreadHistoryRetainedOutputRow[],
  ): void {
    for (const row of rows) {
      const line = eventLine(row);
      this.pendingLines.push(line);
      this.pendingBytes += Buffer.byteLength(line) + 1;
      this.pendingRows += 1;
      this.pendingFirst ??= row.sequence;
      this.pendingLast = row.sequence;
    }
    for (const output of retained) {
      const line = retainedLine(output);
      this.pendingLines.push(line);
      this.pendingBytes += Buffer.byteLength(line) + 1;
    }
  }

  async flushFrame(): Promise<void> {
    if (this.pendingLines.length === 0) return;
    const raw = Buffer.from(`${this.pendingLines.join("\n")}\n`);
    const firstSequence = this.pendingFirst ?? 0;
    const lastSequence = this.pendingLast ?? firstSequence;
    const rows = this.pendingRows;
    this.pendingLines = [];
    this.pendingBytes = 0;
    this.pendingRows = 0;
    this.pendingFirst = null;
    this.pendingLast = null;
    const frame = await zstdCompress(raw, {
      params: { [zlib.constants.ZSTD_c_compressionLevel]: ZSTD_LEVEL },
    });
    const offset = this.offset;
    await this.write(frame);
    this.frames.push({
      compressedBytes: frame.length,
      firstSequence,
      lastSequence,
      offset,
      rawBytes: raw.length,
      rows,
      sha256: createHash("sha256").update(frame).digest("hex"),
    });
    this.totalRows += rows;
    this.totalRawBytes += raw.length;
  }

  async seal(): Promise<{ packedBytes: number; rawBytes: number; rows: number }> {
    await this.flushFrame();
    const table: ThreadHistorySeekTable = {
      formatVersion: THREAD_HISTORY_ARCHIVE_FORMAT_VERSION,
      frames: this.frames,
      rawBytes: this.totalRawBytes,
      rows: this.totalRows,
      threadId: this.threadId,
    };
    const tableBytes = Buffer.from(JSON.stringify(table));
    const trailer = Buffer.alloc(SEEK_TRAILER_BYTES);
    trailer.writeUInt32LE(tableBytes.length, 0);
    trailer.write(SEEK_TRAILER_MAGIC, 4, "ascii");
    const payload = Buffer.concat([tableBytes, trailer]);
    const frameHeader = Buffer.alloc(8);
    frameHeader.writeUInt32LE(SKIPPABLE_FRAME_MAGIC, 0);
    frameHeader.writeUInt32LE(payload.length, 4);
    await this.write(Buffer.concat([frameHeader, payload]));
    await this.handle.sync();
    await this.handle.close();
    await rename(this.tempPath, this.finalPath);
    return {
      packedBytes: this.offset,
      rawBytes: this.totalRawBytes,
      rows: this.totalRows,
    };
  }

  async abort(): Promise<void> {
    await this.handle.close().catch(() => undefined);
    await rm(this.tempPath, { force: true });
  }

  private async write(buffer: Buffer): Promise<void> {
    await this.handle.write(buffer, 0, buffer.length, this.offset);
    this.offset += buffer.length;
  }
}

export async function readThreadHistorySeekTable(
  path: string,
): Promise<ThreadHistorySeekTable> {
  const handle = await open(path, "r");
  try {
    const { size } = await handle.stat();
    if (size < SEEK_TRAILER_BYTES) {
      throw new Error(`Thread history archive ${path} is truncated`);
    }
    const trailer = Buffer.alloc(SEEK_TRAILER_BYTES);
    await handle.read(trailer, 0, SEEK_TRAILER_BYTES, size - SEEK_TRAILER_BYTES);
    if (trailer.toString("ascii", 4) !== SEEK_TRAILER_MAGIC) {
      throw new Error(`Thread history archive ${path} has no seek table`);
    }
    const tableLength = trailer.readUInt32LE(0);
    const tableBytes = Buffer.alloc(tableLength);
    await handle.read(
      tableBytes,
      0,
      tableLength,
      size - SEEK_TRAILER_BYTES - tableLength,
    );
    return seekTableSchema.parse(JSON.parse(tableBytes.toString("utf8")));
  } finally {
    await handle.close();
  }
}

function parseFrameLines(raw: Buffer): ThreadHistoryFrameContents {
  const rows: ThreadHistoryEventRow[] = [];
  const retained: ThreadHistoryRetainedOutputRow[] = [];
  for (const line of raw.toString("utf8").split("\n")) {
    if (line.length === 0) continue;
    const value: unknown = JSON.parse(line);
    const retainedLineResult = retainedLineSchema.safeParse(value);
    if (retainedLineResult.success) {
      const { kind: _kind, ...output } = retainedLineResult.data;
      retained.push(output);
      continue;
    }
    const event = eventLineSchema.parse(value);
    const { kind: _kind, ...columns } = event;
    if ("dataText" in columns) {
      const { dataText, ...rest } = columns;
      rows.push({ ...rest, data: dataText });
    } else {
      const { data, ...rest } = columns;
      rows.push({ ...rest, data: JSON.stringify(data) });
    }
  }
  return { retained, rows };
}

export async function readThreadHistoryFrame(
  handle: FileHandle,
  entry: ThreadHistoryFrameEntry,
): Promise<ThreadHistoryFrameContents> {
  const compressed = Buffer.alloc(entry.compressedBytes);
  await handle.read(compressed, 0, entry.compressedBytes, entry.offset);
  const digest = createHash("sha256").update(compressed).digest("hex");
  if (digest !== entry.sha256) {
    throw new Error(
      `Thread history archive frame at ${entry.offset} failed its checksum`,
    );
  }
  const raw = await zstdDecompress(compressed);
  if (raw.length !== entry.rawBytes) {
    throw new Error(
      `Thread history archive frame at ${entry.offset} decoded to ${raw.length} bytes, expected ${entry.rawBytes}`,
    );
  }
  return parseFrameLines(raw);
}

const EVENT_LINE_MARKER = Buffer.from('{"kind":"event"');

function countEventLines(raw: Buffer): number {
  let count = 0;
  let index = raw.indexOf(EVENT_LINE_MARKER);
  while (index !== -1) {
    if (index === 0 || raw[index - 1] === 0x0a) count += 1;
    index = raw.indexOf(EVENT_LINE_MARKER, index + EVENT_LINE_MARKER.length);
  }
  return count;
}

export async function verifyThreadHistoryFrame(
  handle: FileHandle,
  entry: ThreadHistoryFrameEntry,
): Promise<number> {
  const compressed = Buffer.alloc(entry.compressedBytes);
  await handle.read(compressed, 0, entry.compressedBytes, entry.offset);
  if (createHash("sha256").update(compressed).digest("hex") !== entry.sha256) {
    throw new Error(
      `Thread history archive frame at ${entry.offset} failed its checksum`,
    );
  }
  const raw = await zstdDecompress(compressed);
  if (raw.length !== entry.rawBytes) {
    throw new Error(
      `Thread history archive frame at ${entry.offset} decoded to ${raw.length} bytes, expected ${entry.rawBytes}`,
    );
  }
  return countEventLines(raw);
}

export async function openThreadHistoryArchive(path: string): Promise<{
  close(): Promise<void>;
  handle: FileHandle;
  table: ThreadHistorySeekTable;
}> {
  const table = await readThreadHistorySeekTable(path);
  const handle = await open(path, "r");
  return { close: () => handle.close(), handle, table };
}

export async function verifyThreadHistoryArchive(
  path: string,
  expected: { rows: number; threadId: string },
): Promise<ThreadHistorySeekTable> {
  const archive = await openThreadHistoryArchive(path);
  try {
    if (archive.table.threadId !== expected.threadId) {
      throw new Error(`Thread history archive ${path} belongs to another thread`);
    }
    let rows = 0;
    for (const entry of archive.table.frames) {
      const contents = await readThreadHistoryFrame(archive.handle, entry);
      if (contents.rows.length !== entry.rows) {
        throw new Error(
          `Thread history archive frame at ${entry.offset} has ${contents.rows.length} rows, expected ${entry.rows}`,
        );
      }
      rows += contents.rows.length;
    }
    if (rows !== expected.rows || archive.table.rows !== expected.rows) {
      throw new Error(
        `Thread history archive ${path} has ${rows} rows, expected ${expected.rows}`,
      );
    }
    return archive.table;
  } finally {
    await archive.close();
  }
}

export async function removeThreadHistoryArchive(
  dataDir: string,
  threadId: string,
): Promise<void> {
  const path = threadHistoryArchivePath(dataDir, threadId);
  await rm(path, { force: true });
  await rm(`${path}${TEMP_SUFFIX}`, { force: true });
}

export async function listThreadHistoryArchiveFiles(
  dataDir: string,
): Promise<Array<{ path: string; temporary: boolean; threadId: string }>> {
  let names: string[];
  try {
    names = await readdir(threadHistoryArchiveDir(dataDir));
  } catch (error) {
    if (isMissingFileError(error)) return [];
    throw error;
  }
  return names.flatMap((name) => {
    const temporary = name.endsWith(`${ARCHIVE_EXTENSION}${TEMP_SUFFIX}`);
    if (!temporary && !name.endsWith(ARCHIVE_EXTENSION)) return [];
    const threadId = name.slice(
      0,
      name.length - ARCHIVE_EXTENSION.length - (temporary ? TEMP_SUFFIX.length : 0),
    );
    return [
      {
        path: join(threadHistoryArchiveDir(dataDir), name),
        temporary,
        threadId,
      },
    ];
  });
}

export async function threadHistoryArchiveExists(
  dataDir: string,
  threadId: string,
): Promise<boolean> {
  try {
    await stat(threadHistoryArchivePath(dataDir, threadId));
    return true;
  } catch (error) {
    if (isMissingFileError(error)) return false;
    throw error;
  }
}

function isMissingFileError(error: unknown): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    "code" in error &&
    error.code === "ENOENT"
  );
}

import { readFile, stat, writeFile } from "node:fs/promises";
import { promisify } from "node:util";
import zlib from "node:zlib";
import { sql } from "drizzle-orm";
import { describe, expect, it } from "vitest";
import {
  deleteThread,
  getAppSettings,
  getThreadHistoryArchive,
  pinThread,
  projectAttachmentBackfills,
  setAppSettings,
  threads,
} from "@bb/db";
import { threadScope, turnScope } from "@bb/domain";
import { createBbSdk } from "@bb/sdk/core";
import { createHttpTransport } from "@bb/sdk/node";
import { eq } from "drizzle-orm";
import { getLastProviderThreadId } from "../../src/services/threads/thread-events.js";
import {
  openThreadFile,
  threadHistoryArchivePath,
} from "../../src/services/threads/history-archive/thread-file.js";
import {
  ensureThreadHistoryHot,
  holdThreadHistory,
  isThreadHistoryCold,
  removeOrphanedThreadHistoryFiles,
} from "../../src/services/threads/history-archive/thread-history.js";
import { ThreadHistoryCoolingJob } from "../../src/services/threads/history-archive/thread-history-cooling.js";
import { readJson } from "../helpers/json.js";
import {
  seedEvent,
  seedThreadFixture,
  seedThreadRuntimeState,
} from "../helpers/seed.js";
import { withTestHarness, type TestAppHarness } from "../helpers/test-app.js";

const zstdDecompress = promisify(zlib.zstdDecompress);
const DAY_MS = 86_400_000;

interface StoredRow {
  id: string;
  sequence: number;
  type: string;
  data: string;
}

function allRows(harness: TestAppHarness, threadId: string): StoredRow[] {
  return harness.db.all<StoredRow>(
    sql`SELECT * FROM events WHERE thread_id = ${threadId} ORDER BY sequence`,
  );
}

function seedArchivedConversation(
  harness: TestAppHarness,
  options: { archive?: boolean } = {},
) {
  const { environment, project, thread } = seedThreadFixture(harness);
  harness.db
    .insert(projectAttachmentBackfills)
    .values({
      attemptedAt: 0,
      error: null,
      inputCursor: 0,
      inputId: "",
      inputSequence: 0,
      phase: "done",
      projectId: project.id,
      threadCursor: "",
    })
    .onConflictDoNothing()
    .run();
  seedThreadRuntimeState(harness.deps, {
    environmentId: environment.id,
    inputText: "Summarise the logs",
    providerThreadId: "provider-thread-1",
    threadId: thread.id,
  });
  const turn = {
    threadId: thread.id,
    environmentId: environment.id,
    providerThreadId: "provider-thread-1",
    scope: turnScope("turn-1"),
  };
  let sequence = 3;
  const next = () => sequence++;
  seedEvent(harness.deps, { ...turn, sequence: next(), type: "turn/started", data: {} });
  seedEvent(harness.deps, {
    ...turn,
    sequence: next(),
    type: "item/started",
    data: {
      item: {
        type: "commandExecution",
        id: "command-1",
        command: "cat app.log",
        cwd: "/tmp/test",
        status: "pending",
        approvalStatus: null,
      },
    },
  });
  seedEvent(harness.deps, {
    ...turn,
    sequence: next(),
    type: "item/completed",
    data: {
      item: {
        type: "commandExecution",
        id: "command-1",
        command: "cat app.log",
        cwd: "/tmp/test",
        status: "completed",
        approvalStatus: null,
        aggregatedOutput: "line\n".repeat(400),
        exitCode: 0,
        truncation: {
          aggregatedOutput: {
            originalLength: 2000,
            retainedHeadLength: 2048,
            retainedTailLength: 2048,
            truncatedAt: 1,
          },
        },
      },
    },
  });
  seedEvent(harness.deps, {
    ...turn,
    sequence: next(),
    type: "item/completed",
    data: {
      item: { type: "agentMessage", id: "message-1", text: "The logs are clean." },
    },
  });
  seedEvent(harness.deps, {
    ...turn,
    sequence: next(),
    type: "turn/completed",
    data: { status: "completed" },
  });
  seedEvent(harness.deps, {
    threadId: thread.id,
    environmentId: environment.id,
    providerThreadId: "provider-thread-1",
    scope: threadScope(),
    sequence: next(),
    type: "thread/goal/updated",
    data: {
      providerThreadId: "provider-thread-1",
      objective: "Keep logs clean",
      status: "active",
      tokenBudget: null,
      tokensUsed: 0,
      timeUsedSeconds: 0,
      createdAt: 1,
      updatedAt: 1,
    },
  });
  if (options.archive ?? true) {
    harness.db
      .update(threads)
      .set({ archivedAt: Date.now() - 10 * DAY_MS })
      .where(eq(threads.id, thread.id))
      .run();
  }
  harness.db.run(sql`
    UPDATE events
    SET data = replace(data, '"retainedHeadLength":2048', '"retainedHeadLength":2048.0')
    WHERE thread_id = ${thread.id} AND item_id = 'command-1' AND type = 'item/completed'
  `);
  harness.db.run(sql`DELETE FROM thread_pruning_work`);
  return { environment, project, thread };
}

function cold(harness: TestAppHarness, threadId: string): boolean {
  return isThreadHistoryCold(harness.db, threadId);
}

async function coolNow(harness: TestAppHarness, threadId: string): Promise<boolean> {
  harness.db.run(sql`UPDATE thread_history_archives SET restored_at = NULL`);
  const job = new ThreadHistoryCoolingJob(harness.deps);
  for (let step = 0; step < 1000; step += 1) {
    if (getThreadHistoryArchive(harness.db, threadId)?.state === "cold") return true;
    if ((await job.step()) === "idle") break;
  }
  return getThreadHistoryArchive(harness.db, threadId)?.state === "cold";
}

async function decodeThreadFile(bytes: Buffer): Promise<string[]> {
  const size = bytes.length;
  const tableLength = bytes.readUInt32LE(size - 12);
  const table = JSON.parse(
    bytes.subarray(size - 12 - tableLength, size - 12).toString(),
  ) as { frames: Array<{ compressedBytes: number; offset: number }>; headerBytes: number };
  const regions = [
    bytes.subarray(0, table.headerBytes),
    ...table.frames.map((frame) =>
      bytes.subarray(frame.offset, frame.offset + frame.compressedBytes),
    ),
  ];
  const text = (await Promise.all(regions.map((region) => zstdDecompress(region))))
    .map((buffer) => buffer.toString())
    .join("");
  return text.split("\n").filter((line) => line.length > 0);
}

function withFormatVersion(bytes: Buffer, formatVersion: number): Buffer {
  const size = bytes.length;
  const tableLength = bytes.readUInt32LE(size - 12);
  const tableStart = size - 12 - tableLength;
  const table = JSON.parse(bytes.subarray(tableStart, size - 12).toString()) as Record<
    string,
    unknown
  >;
  const tableBytes = Buffer.from(JSON.stringify({ ...table, formatVersion }));
  const trailer = Buffer.alloc(12);
  trailer.writeUInt32LE(tableBytes.length, 0);
  trailer.write("BBHSEEK1", 4, "ascii");
  const payload = Buffer.concat([tableBytes, trailer]);
  const frameHeader = Buffer.alloc(8);
  frameHeader.writeUInt32LE(0x184d2a5e, 0);
  frameHeader.writeUInt32LE(payload.length, 4);
  return Buffer.concat([bytes.subarray(0, tableStart - 8), frameHeader, payload]);
}

async function exportThread(harness: TestAppHarness, threadId: string): Promise<Buffer> {
  const response = await harness.app.request(`/api/v1/threads/${threadId}/export`);
  expect(response.status).toBe(200);
  expect(response.headers.get("content-type")).toBe("application/zstd");
  return Buffer.from(await response.arrayBuffer());
}

function importThread(harness: TestAppHarness, bytes: Buffer): Promise<Response> {
  const form = new FormData();
  form.set("file", new Blob([Uint8Array.from(bytes)]), "thread.jsonl.zst");
  return Promise.resolve(
    harness.app.request("/api/v1/threads/import", { body: form, method: "POST" }),
  );
}

function eventLines(lines: readonly string[]): string[] {
  return lines.filter((line) => !line.startsWith('{"kind":"header"'));
}

describe("thread history cold tier", () => {
  it("moves history into a full, greppable thread file and restores it byte-for-byte", async () => {
    await withTestHarness(async (harness) => {
      const { thread } = seedArchivedConversation(harness);
      const before = allRows(harness, thread.id);
      expect(await coolNow(harness, thread.id)).toBe(true);

      const remaining = allRows(harness, thread.id);
      expect(remaining.map((row) => row.type)).toEqual([
        "thread/identity",
        "thread/goal/updated",
      ]);
      expect(getThreadHistoryArchive(harness.db, thread.id)?.rowCount).toBe(
        before.length,
      );

      const path = threadHistoryArchivePath(harness.config.dataDir, thread.id);
      const file = await openThreadFile(path);
      expect(file.header).toMatchObject({ threadId: thread.id, providerId: thread.providerId });
      await file.close();
      const lines = await decodeThreadFile(await readFile(path));
      expect(JSON.parse(lines[0] ?? "")).toMatchObject({
        kind: "header",
        format: "bb.thread",
        formatVersion: 1,
        threadId: thread.id,
      });
      expect(eventLines(lines)).toHaveLength(before.length);
      expect(lines.join("\n")).toContain('"text":"The logs are clean."');
      expect(lines.join("\n")).toContain('"dataText":');

      await ensureThreadHistoryHot(harness.deps, thread.id);
      expect(allRows(harness, thread.id)).toEqual(before);
      expect(cold(harness, thread.id)).toBe(false);
      await expect(stat(path)).rejects.toThrow();
    });
  });

  it("serves every event-reading route identically, restoring the thread on first read", async () => {
    await withTestHarness(async (harness) => {
      const { thread } = seedArchivedConversation(harness);
      const messageSeq = allRows(harness, thread.id).find(
        (row) => row.type === "item/completed" && row.data.includes("agentMessage"),
      )?.sequence;
      const paths = [
        `/api/v1/threads/${thread.id}/timeline`,
        `/api/v1/threads/${thread.id}/events?limit=100`,
        `/api/v1/threads/${thread.id}/output`,
        `/api/v1/threads/${thread.id}/context`,
        `/api/v1/threads/${thread.id}/conversation-outline`,
        `/api/v1/threads/${thread.id}/messages/${messageSeq}`,
        `/api/v1/threads/${thread.id}/timeline/turn-summary-details?turnId=turn-1&sourceSeqStart=3&sourceSeqEnd=7`,
      ];
      const fetchJson = async (path: string) => {
        const response = await harness.app.request(path);
        expect(response.status, path).toBe(200);
        return readJson(response);
      };
      const baseline = new Map<string, unknown>();
      for (const path of paths) baseline.set(path, await fetchJson(path));
      const rowCount = allRows(harness, thread.id).length;

      for (const path of paths) {
        expect(await coolNow(harness, thread.id)).toBe(true);
        expect(await fetchJson(path), path).toEqual(baseline.get(path));
        expect(allRows(harness, thread.id)).toHaveLength(rowCount);
        expect(cold(harness, thread.id)).toBe(false);
      }
    });
  });

  it("appends to a cold thread after its last sequence and merges on restore", async () => {
    await withTestHarness(async (harness) => {
      const { environment, thread } = seedArchivedConversation(harness);
      const before = allRows(harness, thread.id);
      const lastSequence = before.at(-1)?.sequence ?? 0;
      await coolNow(harness, thread.id);
      seedEvent(harness.deps, {
        threadId: thread.id,
        environmentId: environment.id,
        providerThreadId: "provider-thread-1",
        scope: threadScope(),
        sequence: lastSequence + 1,
        type: "thread/goal/cleared",
        data: {},
      });
      await ensureThreadHistoryHot(harness.deps, thread.id);
      const after = allRows(harness, thread.id);
      expect(after.map((row) => row.sequence)).toEqual([
        ...before.map((row) => row.sequence),
        lastSequence + 1,
      ]);
    });
  });

  it("keeps provider identity and search segments in the database", async () => {
    await withTestHarness(async (harness) => {
      const { thread } = seedArchivedConversation(harness);
      const segments = () =>
        harness.db.get<{ value: number }>(
          sql`SELECT count(*) AS value FROM thread_search_segments WHERE thread_id = ${thread.id}`,
        )?.value;
      const segmentsBefore = segments();
      const providerThreadId = getLastProviderThreadId(harness.deps, thread.id);
      await coolNow(harness, thread.id);
      expect(getLastProviderThreadId(harness.deps, thread.id)).toBe(
        providerThreadId,
      );
      expect(segments()).toBe(segmentsBefore);
      await ensureThreadHistoryHot(harness.deps, thread.id);
      expect(segments()).toBe(segmentsBefore);
    });
  });

  it("keeps the stored conversation outline when a cold thread's outline is read", async () => {
    await withTestHarness(async (harness) => {
      const { thread } = seedArchivedConversation(harness);
      const path = `/api/v1/threads/${thread.id}/conversation-outline`;
      const before = await readJson(await harness.app.request(path));
      await coolNow(harness, thread.id);
      expect(await readJson(await harness.app.request(path))).toEqual(before);
      const stored = harness.db.get<{ items: string }>(
        sql`SELECT items_json AS items FROM thread_conversation_outlines WHERE thread_id = ${thread.id}`,
      );
      expect(stored?.items).not.toBe("[]");
    });
  });

  it("leaves pinned threads and threads held by an import in the database", async () => {
    await withTestHarness(async (harness) => {
      const { thread } = seedArchivedConversation(harness);
      pinThread(harness.db, harness.hub, { threadId: thread.id });
      expect(await coolNow(harness, thread.id)).toBe(false);
      expect(getThreadHistoryArchive(harness.db, thread.id)).toBeNull();

      const other = seedArchivedConversation(harness).thread;
      const release = holdThreadHistory(harness.db, other.id);
      expect(await coolNow(harness, other.id)).toBe(false);
      release();
      expect(await coolNow(harness, other.id)).toBe(true);
    });
  });

  it("unarchiving restores the history and removes the archive", async () => {
    await withTestHarness(async (harness) => {
      const { thread } = seedArchivedConversation(harness);
      const before = allRows(harness, thread.id);
      await coolNow(harness, thread.id);
      const response = await harness.app.request(
        `/api/v1/threads/${thread.id}/unarchive`,
        { method: "POST" },
      );
      expect(response.status).toBe(200);
      expect(allRows(harness, thread.id)).toEqual(before);
      expect(getThreadHistoryArchive(harness.db, thread.id)).toBeNull();
      await expect(
        stat(threadHistoryArchivePath(harness.config.dataDir, thread.id)),
      ).rejects.toThrow();
    });
  });

  it("removes a deleted thread's archive file in the orphan sweep", async () => {
    await withTestHarness(async (harness) => {
      const { thread } = seedArchivedConversation(harness);
      await coolNow(harness, thread.id);
      const path = threadHistoryArchivePath(harness.config.dataDir, thread.id);
      await stat(path);
      harness.db.run(sql`UPDATE threads SET lifecycle_owner_thread_id = NULL`);
      deleteThread(harness.db, harness.hub, thread.id);
      expect(await removeOrphanedThreadHistoryFiles(harness.deps)).toBe(1);
      await expect(stat(path)).rejects.toThrow();
    });
  });

  it("cools archived threads after the configured delay and never when set to never", async () => {
    await withTestHarness(async (harness) => {
      const { thread } = seedArchivedConversation(harness);
      setAppSettings(harness.db, {
        ...getAppSettings(harness.db),
        coldHistoryAfterArchivedDays: "never",
        coldHistoryAfterIdleDays: "never",
      });
      const job = new ThreadHistoryCoolingJob(harness.deps);
      expect(await job.step()).toBe("idle");
      setAppSettings(harness.db, {
        ...getAppSettings(harness.db),
        coldHistoryAfterArchivedDays: 7,
      });
      while ((await job.step()) === "worked") {
        if (cold(harness, thread.id)) break;
      }
      expect(cold(harness, thread.id)).toBe(true);
    });
  });

  it("exports the same events from a hot and a cold thread without restoring it", async () => {
    await withTestHarness(async (harness) => {
      const { thread } = seedArchivedConversation(harness);
      const hot = eventLines(await decodeThreadFile(await exportThread(harness, thread.id)));
      expect(await coolNow(harness, thread.id)).toBe(true);
      const rowsWhileCold = allRows(harness, thread.id);
      const coldExport = eventLines(
        await decodeThreadFile(await exportThread(harness, thread.id)),
      );
      expect(coldExport).toEqual(hot);
      expect(cold(harness, thread.id)).toBe(true);
      expect(allRows(harness, thread.id)).toEqual(rowsWhileCold);
    });
  });

  it("imports an exported file into its thread through the SDK, adding only the missing events", async () => {
    await withTestHarness(async (harness) => {
      const { thread } = seedArchivedConversation(harness);
      const sdk = createBbSdk({
        transport: createHttpTransport({
          baseUrl: "http://localhost",
          runtime: "node",
          fetch: async (input, init) => harness.app.fetch(new Request(input, init)),
        }),
      });
      const before = allRows(harness, thread.id);
      const bytes = await sdk.threads.experimental_export({ threadId: thread.id });
      harness.db.run(
        sql`DELETE FROM events WHERE thread_id = ${thread.id} AND type = 'item/completed'`,
      );
      expect(await sdk.threads.experimental_import({ file: bytes })).toEqual({
        threadId: thread.id,
        rows: before.length,
        insertedRows: 2,
      });
      expect(allRows(harness, thread.id)).toEqual(before);
    });
  });

  it("imports into a cold thread by restoring it first", async () => {
    await withTestHarness(async (harness) => {
      const { thread } = seedArchivedConversation(harness);
      const before = allRows(harness, thread.id);
      const bytes = await exportThread(harness, thread.id);
      expect(await coolNow(harness, thread.id)).toBe(true);
      const response = await importThread(harness, bytes);
      expect(await readJson(response)).toMatchObject({ insertedRows: 0 });
      expect(cold(harness, thread.id)).toBe(false);
      expect(allRows(harness, thread.id)).toEqual(before);
    });
  });

  it("rejects a thread that is not on this server and an unknown format version", async () => {
    await withTestHarness(async (harness) => {
      const { thread } = seedArchivedConversation(harness);
      const bytes = await exportThread(harness, thread.id);

      const newer = await importThread(harness, withFormatVersion(bytes, 2));
      expect(newer.status).toBe(400);
      expect(JSON.stringify(await readJson(newer))).toContain("format version 2");

      const garbage = await importThread(harness, Buffer.from("not a thread file"));
      expect(garbage.status).toBe(400);

      harness.db.run(sql`UPDATE threads SET lifecycle_owner_thread_id = NULL`);
      deleteThread(harness.db, harness.hub, thread.id);
      const missing = await importThread(harness, bytes);
      expect(missing.status).toBe(404);
      expect(JSON.stringify(await readJson(missing))).toContain("another server");
    });
  });

  it("leaves a thread cold when its archive uses a format version bb cannot read", async () => {
    await withTestHarness(async (harness) => {
      const { thread } = seedArchivedConversation(harness);
      expect(await coolNow(harness, thread.id)).toBe(true);
      const path = threadHistoryArchivePath(harness.config.dataDir, thread.id);
      await writeFile(path, withFormatVersion(await readFile(path), 2));
      const response = await harness.app.request(
        `/api/v1/threads/${thread.id}/timeline`,
      );
      expect(response.status).toBe(409);
      expect(cold(harness, thread.id)).toBe(true);
      expect(getThreadHistoryArchive(harness.db, thread.id)?.state).toBe("cold");
    });
  });
});

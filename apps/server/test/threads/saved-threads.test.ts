import { readFile, stat, writeFile } from "node:fs/promises";
import { promisify } from "node:util";
import zlib from "node:zlib";
import { eq, sql } from "drizzle-orm";
import { describe, expect, it } from "vitest";
import {
  acquireProjectAttachmentOwnership,
  getAppSettings,
  getSavedThread,
  getThread,
  insertThreadTableRow,
  pinThread,
  readThreadTableRows,
  projectAttachmentBackfills,
  setAppSettings,
  setExperiments,
  threads,
  upsertSavedThread,
} from "@bb/db";
import { turnScope } from "@bb/domain";
import { createBbSdk } from "@bb/sdk/core";
import { createHttpTransport } from "@bb/sdk/node";
import { storeAttachment } from "../../src/services/projects/attachments.js";
import {
  reconcileSavedThreads,
  holdSavedThread,
} from "../../src/services/threads/saved-threads/saved-thread-files.js";
import { savedThreadFilePath } from "../../src/services/threads/saved-threads/thread-file.js";
import {
  ThreadSavingJob,
  startThreadSaving,
} from "../../src/services/threads/saved-threads/thread-saving.js";
import { readJson } from "../helpers/json.js";
import {
  seedEvent,
  seedThread,
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

function addConversation(
  harness: TestAppHarness,
  args: { archive?: boolean; environmentId: string; threadId: string },
) {
  seedThreadRuntimeState(harness.deps, {
    environmentId: args.environmentId,
    inputText: "Summarise the logs",
    providerThreadId: `provider-${args.threadId}`,
    threadId: args.threadId,
  });
  const turn = {
    threadId: args.threadId,
    environmentId: args.environmentId,
    providerThreadId: `provider-${args.threadId}`,
    scope: turnScope("turn-1"),
  };
  let sequence = 3;
  const next = () => sequence++;
  seedEvent(harness.deps, { ...turn, sequence: next(), type: "turn/started", data: {} });
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
    data: { item: { type: "agentMessage", id: "message-1", text: "The logs are clean." } },
  });
  seedEvent(harness.deps, {
    ...turn,
    sequence: next(),
    type: "turn/completed",
    data: { status: "completed" },
  });
  harness.db.run(sql`
    UPDATE events
    SET data = replace(data, '"retainedHeadLength":2048', '"retainedHeadLength":2048.0')
    WHERE thread_id = ${args.threadId} AND item_id = 'command-1'
  `);
  if (args.archive ?? true) {
    harness.db
      .update(threads)
      .set({ archivedAt: Date.now() - 10 * DAY_MS })
      .where(eq(threads.id, args.threadId))
      .run();
  }
  harness.db.run(sql`DELETE FROM thread_pruning_work`);
}

function seedArchivedConversation(
  harness: TestAppHarness,
  options: { archive?: boolean } = {},
) {
  const fixture = seedThreadFixture(harness);
  setExperiments(harness.db, { savedThreads: true });
  harness.db
    .insert(projectAttachmentBackfills)
    .values({
      attemptedAt: 0,
      error: null,
      inputCursor: 0,
      inputId: "",
      inputSequence: 0,
      phase: "done",
      projectId: fixture.project.id,
      threadCursor: "",
    })
    .onConflictDoNothing()
    .run();
  addConversation(harness, {
    archive: options.archive,
    environmentId: fixture.environment.id,
    threadId: fixture.thread.id,
  });
  return fixture;
}

function savedState(harness: TestAppHarness, threadId: string) {
  return getSavedThread(harness.db, threadId)?.state ?? null;
}

async function runJob(harness: TestAppHarness, job = new ThreadSavingJob(harness.deps)) {
  for (let step = 0; step < 2000; step += 1) {
    if ((await job.step()) === "idle") return job;
  }
  throw new Error("Thread saving job did not settle");
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

function sdkFor(harness: TestAppHarness) {
  return createBbSdk({
    transport: createHttpTransport({
      baseUrl: "http://localhost",
      runtime: "node",
      fetch: async (input, init) => harness.app.fetch(new Request(input, init)),
    }),
  });
}

function importThread(harness: TestAppHarness, bytes: Buffer): Promise<Response> {
  const form = new FormData();
  form.set("file", new Blob([Uint8Array.from(bytes)]), "thread.jsonl.zst");
  return Promise.resolve(
    harness.app.request("/api/v1/threads/import", { body: form, method: "POST" }),
  );
}

describe("saved threads", () => {
  it("saves an archived thread to a full file and removes it from the database", async () => {
    await withTestHarness(async (harness) => {
      const { project, thread } = seedArchivedConversation(harness);
      const before = allRows(harness, thread.id);
      harness.db.run(sql`
        INSERT INTO thread_plugin_metadata (thread_id, plugin_id, metadata_json)
        VALUES (${thread.id}, 'example', '{"pr":42}')
      `);
      const attachment = await storeAttachment(
        harness.db,
        harness.config.dataDir,
        project.id,
        new File(["hello attachment"], "note.txt", { type: "text/plain" }),
      );
      acquireProjectAttachmentOwnership(harness.db, thread.id, [attachment.path]);

      await runJob(harness);

      expect(savedState(harness, thread.id)).toBe("saved");
      expect(getThread(harness.db, thread.id)).toBeNull();
      expect(allRows(harness, thread.id)).toEqual([]);
      const lines = await decodeThreadFile(
        await readFile(savedThreadFilePath(harness.config.dataDir, thread.id)),
      );
      expect(JSON.parse(lines[0] ?? "")).toMatchObject({
        kind: "header",
        format: "bb.thread",
        formatVersion: 1,
        threadId: thread.id,
        title: "Test Thread",
      });
      const kinds = lines.map((line) => (JSON.parse(line) as { kind: string }).kind);
      expect(kinds.filter((kind) => kind === "event")).toHaveLength(before.length);
      expect(lines.join("\n")).toContain('"table":"threads"');
      expect(lines.join("\n")).toContain('"table":"thread_plugin_metadata"');
      expect(lines.join("\n")).toContain('"kind":"attachment"');
      expect(lines.join("\n")).toContain('"dataText":');

      const listed = await readJson(await harness.app.request("/api/v1/saved-threads"));
      expect(listed).toMatchObject({
        hasMore: false,
        savedThreads: [{ threadId: thread.id, state: "saved", rows: before.length }],
      });
    });
  });

  it("restores a saved thread byte-for-byte with its metadata and attachments", async () => {
    await withTestHarness(async (harness) => {
      const { project, thread } = seedArchivedConversation(harness);
      const before = allRows(harness, thread.id);
      const threadBefore = getThread(harness.db, thread.id);
      harness.db.run(sql`
        INSERT INTO thread_plugin_metadata (thread_id, plugin_id, metadata_json)
        VALUES (${thread.id}, 'example', '{"pr":42}')
      `);
      const attachment = await storeAttachment(
        harness.db,
        harness.config.dataDir,
        project.id,
        new File(["hello attachment"], "note.txt", { type: "text/plain" }),
      );
      acquireProjectAttachmentOwnership(harness.db, thread.id, [attachment.path]);
      await runJob(harness);
      harness.db.run(sql`DELETE FROM project_attachments`);

      const restored = await sdkFor(harness).threads.experimental_restoreSaved({
        threadId: thread.id,
      });
      expect(restored).toEqual({
        threadId: thread.id,
        rows: before.length,
        insertedRows: before.length,
      });
      expect(allRows(harness, thread.id)).toEqual(before);
      expect(getThread(harness.db, thread.id)).toMatchObject({
        archivedAt: threadBefore?.archivedAt,
        title: threadBefore?.title,
        deletedAt: null,
      });
      expect(
        harness.db.get<{ metadata: string }>(
          sql`SELECT metadata_json AS metadata FROM thread_plugin_metadata WHERE thread_id = ${thread.id}`,
        )?.metadata,
      ).toBe('{"pr":42}');
      expect(
        harness.db.get<{ n: number }>(
          sql`SELECT count(*) AS n FROM project_attachment_threads WHERE thread_id = ${thread.id}`,
        )?.n,
      ).toBe(1);
      expect(savedState(harness, thread.id)).toBe("restored");
      await expect(
        stat(savedThreadFilePath(harness.config.dataDir, thread.id)),
      ).rejects.toThrow();

      await runJob(harness);
      expect(getThread(harness.db, thread.id)).not.toBeNull();
    });
  });

  it("serves a restored thread's timeline the same as before it was saved", async () => {
    await withTestHarness(async (harness) => {
      const { thread } = seedArchivedConversation(harness);
      const path = `/api/v1/threads/${thread.id}/timeline`;
      const before = await readJson(await harness.app.request(path));
      await runJob(harness);
      expect((await harness.app.request(path)).status).toBe(404);
      await harness.app.request(`/api/v1/saved-threads/${thread.id}/restore`, {
        method: "POST",
      });
      expect(await readJson(await harness.app.request(path))).toEqual(before);
    });
  });

  it("leaves pinned threads, held threads and parents of threads still in the database", async () => {
    await withTestHarness(async (harness) => {
      const { environment, project, thread } = seedArchivedConversation(harness);
      pinThread(harness.db, harness.hub, { threadId: thread.id });
      const parent = seedThread(harness.deps, {
        projectId: project.id,
        environmentId: environment.id,
      });
      addConversation(harness, { environmentId: environment.id, threadId: parent.id });
      const child = seedThread(harness.deps, {
        projectId: project.id,
        environmentId: environment.id,
        parentThreadId: parent.id,
      });
      addConversation(harness, { environmentId: environment.id, threadId: child.id });
      const release = holdSavedThread(harness.db, child.id);

      await runJob(harness);
      expect(savedState(harness, thread.id)).toBeNull();
      expect(savedState(harness, child.id)).toBeNull();
      expect(savedState(harness, parent.id)).toBeNull();

      release();
      await runJob(harness);
      expect(savedState(harness, child.id)).toBe("saved");
      await runJob(harness);
      expect(savedState(harness, parent.id)).toBe("saved");

      await sdkFor(harness).threads.experimental_restoreSaved({ threadId: child.id });
      expect(getThread(harness.db, parent.id)).not.toBeNull();
      expect(getThread(harness.db, child.id)?.parentThreadId).toBe(parent.id);
    });
  });

  it("saves a thread on request through the SDK and refuses a pinned one", async () => {
    await withTestHarness(async (harness) => {
      const { thread } = seedArchivedConversation(harness, { archive: false });
      const saving = startThreadSaving(harness.deps, { isFrozen: () => false });
      try {
        const sdk = sdkFor(harness);
        expect(await sdk.threads.experimental_save({ threadId: thread.id })).toMatchObject({
          state: "saving",
          threadId: thread.id,
        });
        for (let attempt = 0; attempt < 200 && savedState(harness, thread.id) !== "saved"; attempt += 1) {
          await new Promise((resolve) => setTimeout(resolve, 10));
        }
        expect(savedState(harness, thread.id)).toBe("saved");

        const other = seedArchivedConversation(harness).thread;
        pinThread(harness.db, harness.hub, { threadId: other.id });
        const refused = await harness.app.request(`/api/v1/threads/${other.id}/save`, {
          method: "POST",
        });
        expect(refused.status).toBe(409);
      } finally {
        await saving.stop();
      }
    });
  });

  it("exports through the SDK and imports into the same thread, adding only missing events", async () => {
    await withTestHarness(async (harness) => {
      const { thread } = seedArchivedConversation(harness);
      const sdk = sdkFor(harness);
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

  it("imports an exported file of a deleted thread by recreating it", async () => {
    await withTestHarness(async (harness) => {
      const { thread } = seedArchivedConversation(harness);
      const before = allRows(harness, thread.id);
      const bytes = Buffer.from(
        await sdkFor(harness).threads.experimental_export({ threadId: thread.id }),
      );
      harness.db.delete(threads).where(eq(threads.id, thread.id)).run();
      const response = await importThread(harness, bytes);
      expect(response.status).toBe(200);
      expect(allRows(harness, thread.id)).toEqual(before);
      expect(getThread(harness.db, thread.id)?.title).toBe("Test Thread");
    });
  });

  it("rejects an unknown format version, a file that isn't a thread file, and a missing project", async () => {
    await withTestHarness(async (harness) => {
      const { project, thread } = seedArchivedConversation(harness);
      const bytes = Buffer.from(
        await sdkFor(harness).threads.experimental_export({ threadId: thread.id }),
      );
      const newer = await importThread(harness, withFormatVersion(bytes, 2));
      expect(newer.status).toBe(400);
      expect(JSON.stringify(await readJson(newer))).toContain("format version 2");
      expect((await importThread(harness, Buffer.from("not a thread file"))).status).toBe(400);

      harness.db.delete(threads).where(eq(threads.id, thread.id)).run();
      harness.db.run(sql`UPDATE projects SET deleted_at = 1 WHERE id = ${project.id}`);
      const missing = await importThread(harness, bytes);
      expect(missing.status).toBe(404);
      expect(JSON.stringify(await readJson(missing))).toContain("another server");
    });
  });

  it("keeps a saved thread listed when its restore fails", async () => {
    await withTestHarness(async (harness) => {
      const { thread } = seedArchivedConversation(harness);
      await runJob(harness);
      const path = savedThreadFilePath(harness.config.dataDir, thread.id);
      await writeFile(path, withFormatVersion(await readFile(path), 2));
      const response = await harness.app.request(
        `/api/v1/saved-threads/${thread.id}/restore`,
        { method: "POST" },
      );
      expect(response.status).toBe(400);
      expect(savedState(harness, thread.id)).toBe("saved");
      expect(getThread(harness.db, thread.id)).toBeNull();
      await stat(path);
    });
  });

  it("recovers an interrupted save and an interrupted restore at startup", async () => {
    await withTestHarness(async (harness) => {
      const { thread } = seedArchivedConversation(harness);
      const threadRow = readThreadTableRows(harness.db, thread.id).find(
        (row) => row.table === "threads",
      );
      if (threadRow === undefined) throw new Error("expected a thread row");
      await runJob(harness);
      const saved = getSavedThread(harness.db, thread.id);
      if (saved === null) throw new Error("expected a saved thread");
      upsertSavedThread(harness.db, { ...saved, state: "restoring" });
      insertThreadTableRow(harness.db, threadRow);
      await reconcileSavedThreads(harness.deps);
      expect(savedState(harness, thread.id)).toBe("saved");
      expect(getThread(harness.db, thread.id)).toBeNull();
    });
  });

  it("does nothing new with the experiment off or the setting at never, but still restores", async () => {
    await withTestHarness(async (harness) => {
      const { thread } = seedArchivedConversation(harness);
      setAppSettings(harness.db, {
        ...getAppSettings(harness.db),
        saveArchivedThreadsAfterDays: "never",
      });
      await runJob(harness);
      expect(savedState(harness, thread.id)).toBeNull();
      setAppSettings(harness.db, {
        ...getAppSettings(harness.db),
        saveArchivedThreadsAfterDays: 7,
      });
      await runJob(harness);
      expect(savedState(harness, thread.id)).toBe("saved");

      setExperiments(harness.db, { savedThreads: false });
      const other = seedArchivedConversation(harness).thread;
      setExperiments(harness.db, { savedThreads: false });
      await runJob(harness);
      expect(savedState(harness, other.id)).toBeNull();
      expect(
        (await harness.app.request(`/api/v1/threads/${other.id}/export`)).status,
      ).toBe(403);
      expect(
        (await harness.app.request(`/api/v1/threads/${other.id}/save`, { method: "POST" }))
          .status,
      ).toBe(403);
      const restore = await harness.app.request(
        `/api/v1/saved-threads/${thread.id}/restore`,
        { method: "POST" },
      );
      expect(restore.status).toBe(200);
      expect(getThread(harness.db, thread.id)).not.toBeNull();
    });
  });
});

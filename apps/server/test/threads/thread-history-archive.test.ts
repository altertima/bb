import { readFile, stat } from "node:fs/promises";
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
import { eq } from "drizzle-orm";
import { getLastProviderThreadId } from "../../src/services/threads/thread-events.js";
import {
  readThreadHistorySeekTable,
  threadHistoryArchivePath,
} from "../../src/services/threads/history-archive/archive-file.js";
import {
  ensureThreadHistoryHot,
  removeOrphanedThreadHistoryFiles,
  threadHistoryStatus,
} from "../../src/services/threads/history-archive/thread-history.js";
import {
  ThreadHistoryCoolingJob,
  startThreadHistoryMaintenance,
} from "../../src/services/threads/history-archive/thread-history-cooling.js";
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

async function coolNow(harness: TestAppHarness, threadId: string) {
  const job = new ThreadHistoryCoolingJob(harness.deps);
  let settled = false;
  const cooled = job.requestCooling(threadId).finally(() => {
    settled = true;
  });
  while (!settled) await job.step();
  await cooled;
}

describe("thread history cold tier", () => {
  it("moves history to a greppable archive file and restores it byte-for-byte", async () => {
    await withTestHarness(async (harness) => {
      const { thread } = seedArchivedConversation(harness);
      const before = allRows(harness, thread.id);
      await coolNow(harness, thread.id);

      const remaining = allRows(harness, thread.id);
      expect(remaining.map((row) => row.type)).toEqual([
        "thread/identity",
        "thread/goal/updated",
      ]);
      expect(threadHistoryStatus(harness.deps, thread.id)).toMatchObject({
        storage: "cold",
        archivedRows: before.length - 2,
      });

      const path = threadHistoryArchivePath(harness.config.dataDir, thread.id);
      const file = await readFile(path);
      const table = await readThreadHistorySeekTable(path);
      const regions = [
        file.subarray(0, table.frames[0]?.offset ?? file.length),
        ...table.frames.map((frame) =>
          file.subarray(frame.offset, frame.offset + frame.compressedBytes),
        ),
      ];
      const text = (
        await Promise.all(regions.map((region) => zstdDecompress(region)))
      )
        .map((buffer) => buffer.toString())
        .join("");
      const lines = text.split("\n").filter((line) => line.length > 0);
      expect(JSON.parse(lines[0] ?? "")).toMatchObject({
        kind: "header",
        threadId: thread.id,
      });
      expect(text).toContain('"text":"The logs are clean."');
      expect(text).toContain('"dataText":');

      await ensureThreadHistoryHot(harness.deps, thread.id);
      expect(allRows(harness, thread.id)).toEqual(before);
      expect(threadHistoryStatus(harness.deps, thread.id).storage).toBe("hot");
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
        await coolNow(harness, thread.id);
        expect(threadHistoryStatus(harness.deps, thread.id).storage).toBe("cold");
        expect(await fetchJson(path), path).toEqual(baseline.get(path));
        expect(allRows(harness, thread.id)).toHaveLength(rowCount);
        expect(threadHistoryStatus(harness.deps, thread.id).storage).toBe("hot");
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

  it("refuses to cool a pinned thread or one open in a client", async () => {
    await withTestHarness(async (harness) => {
      const { thread } = seedArchivedConversation(harness);
      pinThread(harness.db, harness.hub, { threadId: thread.id });
      await expect(coolNow(harness, thread.id)).rejects.toThrow(/can't be archived/);
      expect(getThreadHistoryArchive(harness.db, thread.id)).toBeNull();
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
        if (threadHistoryStatus(harness.deps, thread.id).storage === "cold") break;
      }
      expect(threadHistoryStatus(harness.deps, thread.id).storage).toBe("cold");
    });
  });

  it("cools and restores through the history routes", async () => {
    await withTestHarness(async (harness) => {
      const { thread } = seedArchivedConversation(harness);
      const maintenance = startThreadHistoryMaintenance(harness.deps, {
        isFrozen: () => false,
      });
      try {
        const cooled = await harness.app.request(
          `/api/v1/threads/${thread.id}/history/cool`,
          { method: "POST" },
        );
        expect(cooled.status).toBe(200);
        expect(await readJson(cooled)).toMatchObject({ storage: "cold" });
        const status = await harness.app.request(
          `/api/v1/threads/${thread.id}/history`,
        );
        expect(await readJson(status)).toMatchObject({ storage: "cold" });
        const restored = await harness.app.request(
          `/api/v1/threads/${thread.id}/history/restore`,
          { method: "POST" },
        );
        expect(await readJson(restored)).toMatchObject({ storage: "hot" });
      } finally {
        await maintenance.stop();
      }
    });
  });
});

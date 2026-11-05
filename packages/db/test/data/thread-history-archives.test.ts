import { describe, expect, it } from "vitest";
import { sql } from "drizzle-orm";
import { threadScope, turnScope, type ThreadEventType } from "@bb/domain";
import { createEnvironment } from "../../src/data/environments.js";
import { insertEvents } from "../../src/data/events.js";
import { upsertHost } from "../../src/data/hosts.js";
import { createProject } from "../../src/data/projects.js";
import { createQueuedThreadMessage } from "../../src/data/queued-thread-messages.js";
import {
  THREAD_HISTORY_ARCHIVE_FORMAT_VERSION,
  deleteThreadHistoryColdWindow,
  insertThreadHistoryRows,
  isThreadHistoryCoolable,
  listThreadHistoryCoolingCandidates,
  readThreadHistoryColdWindow,
  upsertThreadHistoryArchive,
  type ThreadHistoryCoolingCriteria,
} from "../../src/data/thread-history-archives.js";
import { markThreadPruningPolicyWork } from "../../src/data/thread-pruning-work.js";
import { archiveThread, createThread, pinThread } from "../../src/data/threads.js";
import { noopNotifier } from "../../src/notifier.js";
import { projectAttachmentBackfills } from "../../src/schema.js";
import { createMigratedConnection } from "../../src/testing.js";

type Db = ReturnType<typeof createMigratedConnection>;

function setup() {
  const db = createMigratedConnection();
  const host = upsertHost(db, noopNotifier, { name: "test-host" });
  const { project } = createProject(db, noopNotifier, {
    name: "test-project",
    source: { type: "local_path", hostId: host.id, path: "/tmp/test" },
  });
  const environment = createEnvironment(db, noopNotifier, {
    projectId: project.id,
    hostId: host.id,
    path: "/tmp/environment",
    providerOwnsPath: false,
    status: "ready",
    environmentProvider: null,
  });
  db.insert(projectAttachmentBackfills)
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
    .run();
  return { db, environment, project };
}

function seedThread(
  context: ReturnType<typeof setup>,
  options: { archived?: boolean } = {},
) {
  const thread = createThread(context.db, noopNotifier, {
    projectId: context.project.id,
    providerId: "codex",
    environmentId: context.environment.id,
    status: "idle",
  });
  const rows: Array<{ type: ThreadEventType; data: unknown; turn?: boolean }> = [
    { type: "thread/identity", data: {} },
    { type: "turn/started", data: {}, turn: true },
    {
      type: "item/completed",
      turn: true,
      data: {
        item: { type: "agentMessage", id: "message-1", text: "first reply" },
      },
    },
    {
      type: "item/completed",
      turn: true,
      data: {
        item: {
          type: "commandExecution",
          id: "command-1",
          command: "echo hi",
          cwd: "/tmp",
          status: "completed",
          approvalStatus: null,
          aggregatedOutput: "hi",
          exitCode: 0,
        },
      },
    },
    { type: "turn/completed", data: { status: "completed" }, turn: true },
  ];
  insertEvents(
    context.db,
    noopNotifier,
    rows.map((row, index) => ({
      threadId: thread.id,
      environmentId: context.environment.id,
      providerThreadId: "provider-thread-1",
      scope: row.turn ? turnScope("turn-1") : threadScope(),
      sequence: index + 1,
      type: row.type,
      itemId: null,
      itemKind: null,
      parentToolCallId: null,
      data: JSON.stringify(row.data),
    })),
  );
  if (options.archived ?? true) archiveThread(context.db, noopNotifier, thread.id);
  drainPruningWork(context.db);
  return thread;
}

function drainPruningWork(db: Db): void {
  db.run(sql`DELETE FROM thread_pruning_work`);
}

function criteria(): ThreadHistoryCoolingCriteria {
  return { archivedBefore: Date.now() + 1000, idleBefore: null };
}

function coolable(db: Db, threadId: string): boolean {
  return isThreadHistoryCoolable(db, { ...criteria(), threadId });
}

describe("thread history cooling eligibility", () => {
  it("selects an archived thread whose project finished its attachment backfill", () => {
    const context = setup();
    const thread = seedThread(context);
    expect(
      listThreadHistoryCoolingCandidates(context.db, {
        ...criteria(),
        afterCursor: 0,
        scanLimit: 10,
      }).threadIds,
    ).toEqual([thread.id]);
    expect(
      listThreadHistoryCoolingCandidates(context.db, {
        archivedBefore: null,
        idleBefore: null,
        afterCursor: 0,
        scanLimit: 10,
      }).threadIds,
    ).toEqual([]);
  });

  it("pages through threads past ineligible ones and ends the pass", () => {
    const context = setup();
    const first = seedThread(context);
    seedThread(context, { archived: false });
    const last = seedThread(context);
    const page = (afterCursor: number) =>
      listThreadHistoryCoolingCandidates(context.db, {
        ...criteria(),
        afterCursor,
        scanLimit: 2,
      });
    const firstPage = page(0);
    expect(firstPage.threadIds).toEqual([first.id]);
    expect(firstPage.nextCursor).not.toBeNull();
    const secondPage = page(firstPage.nextCursor ?? 0);
    expect(secondPage.threadIds).toEqual([last.id]);
    expect(page(secondPage.nextCursor ?? 0)).toEqual({
      nextCursor: null,
      threadIds: [],
    });
  });

  it("skips live threads unless the idle rule covers them", () => {
    const context = setup();
    const thread = seedThread(context, { archived: false });
    expect(coolable(context.db, thread.id)).toBe(false);
    expect(
      isThreadHistoryCoolable(context.db, {
        archivedBefore: null,
        idleBefore: Date.now() + 1000,
        threadId: thread.id,
      }),
    ).toBe(true);
  });

  it("skips projects whose attachment backfill is not done", () => {
    const context = setup();
    const thread = seedThread(context);
    context.db.run(
      sql`UPDATE project_attachment_backfills SET phase = 'events' WHERE project_id = ${context.project.id}`,
    );
    expect(coolable(context.db, thread.id)).toBe(false);
  });

  it("skips pinned, running, queued, prompting and pruning-pending threads", () => {
    const context = setup();
    const pinned = seedThread(context);
    pinThread(context.db, noopNotifier, { threadId: pinned.id });
    expect(coolable(context.db, pinned.id)).toBe(false);

    const running = seedThread(context);
    context.db.run(sql`UPDATE threads SET status = 'active' WHERE id = ${running.id}`);
    expect(coolable(context.db, running.id)).toBe(false);

    const queued = seedThread(context);
    createQueuedThreadMessage(context.db, noopNotifier, {
      threadId: queued.id,
      content: [{ type: "text", text: "later", mentions: [] }],
      model: "gpt-5",
      reasoningLevel: "medium",
      permissionMode: "full",
      senderThreadId: null,
      serviceTier: "default",
      waitingOn: null,
      sendAt: null,
      payload: { kind: "inline" },
      systemNotice: null,
    });
    expect(coolable(context.db, queued.id)).toBe(false);

    const prompting = seedThread(context);
    context.db.run(sql`
      INSERT INTO pending_interactions (id, thread_id, origin_kind, status, payload, created_at, updated_at)
      VALUES ('interaction-1', ${prompting.id}, 'provider', 'pending', '{}', 0, 0)
    `);
    expect(coolable(context.db, prompting.id)).toBe(false);

    const pruning = seedThread(context);
    markThreadPruningPolicyWork(context.db, [pruning.id], "usage");
    expect(coolable(context.db, pruning.id)).toBe(false);
  });

  it("skips a thread with an unfinished background task", () => {
    const context = setup();
    const thread = seedThread(context);
    insertEvents(context.db, noopNotifier, [
      {
        threadId: thread.id,
        environmentId: context.environment.id,
        providerThreadId: "provider-thread-1",
        scope: turnScope("turn-1"),
        sequence: 6,
        type: "item/started",
        itemId: "task-1",
        itemKind: "backgroundTask",
        parentToolCallId: null,
        data: JSON.stringify({
          item: {
            type: "backgroundTask",
            id: "task-1",
            title: "build",
            status: "pending",
          },
        }),
      },
    ]);
    drainPruningWork(context.db);
    expect(coolable(context.db, thread.id)).toBe(false);
  });

  it("skips archives that are not hot, and hot ones restored too recently", () => {
    const context = setup();
    const thread = seedThread(context);
    const archive = {
      coldAt: null,
      coldBeforeSequence: 5,
      cursorSequence: 0,
      formatVersion: THREAD_HISTORY_ARCHIVE_FORMAT_VERSION,
      packedBytes: 0,
      rawBytes: 0,
      restoredAt: null,
      rowCount: 0,
      state: "cold" as const,
      threadId: thread.id,
      updatedAt: 0,
    };
    upsertThreadHistoryArchive(context.db, archive);
    expect(coolable(context.db, thread.id)).toBe(false);
    upsertThreadHistoryArchive(context.db, {
      ...archive,
      state: "hot",
      restoredAt: Date.now() + 5000,
    });
    expect(coolable(context.db, thread.id)).toBe(false);
    upsertThreadHistoryArchive(context.db, {
      ...archive,
      state: "hot",
      restoredAt: 1,
    });
    expect(coolable(context.db, thread.id)).toBe(true);
  });
});

describe("thread history cold windows", () => {
  it("leaves keep-hot rows and the latest row in the database and restores the rest byte-for-byte", () => {
    const context = setup();
    const thread = seedThread(context);
    const before = context.db.all<{ id: string; sequence: number; data: string }>(
      sql`SELECT * FROM events WHERE thread_id = ${thread.id} ORDER BY sequence`,
    );
    const window = readThreadHistoryColdWindow(context.db, {
      afterSequence: 0,
      beforeSequence: 5,
      maxBytes: 1024 * 1024,
      maxRows: 100,
      threadId: thread.id,
    });
    expect(window.rows.map((row) => row.sequence)).toEqual([2, 3, 4]);

    const deleted = deleteThreadHistoryColdWindow(context.db, {
      afterSequence: 0,
      beforeSequence: 5,
      maxBytes: 1024 * 1024,
      maxRows: 2,
      threadId: thread.id,
    });
    expect(deleted).toMatchObject({ deleted: 2, lastSequence: 3 });
    deleteThreadHistoryColdWindow(context.db, {
      afterSequence: 3,
      beforeSequence: 5,
      maxBytes: 1024 * 1024,
      maxRows: 2,
      threadId: thread.id,
    });
    expect(
      context.db
        .all<{ sequence: number }>(
          sql`SELECT sequence FROM events WHERE thread_id = ${thread.id} ORDER BY sequence`,
        )
        .map((row) => row.sequence),
    ).toEqual([1, 5]);

    expect(
      insertThreadHistoryRows(context.db, {
        now: Date.now(),
        retained: window.retained,
        rows: [...window.rows, ...window.rows],
      }),
    ).toBe(3);
    expect(
      context.db.all(
        sql`SELECT * FROM events WHERE thread_id = ${thread.id} ORDER BY sequence`,
      ),
    ).toEqual(before);
  });

  it("stops a window at the byte cap but always takes one row", () => {
    const context = setup();
    const thread = seedThread(context);
    const window = readThreadHistoryColdWindow(context.db, {
      afterSequence: 0,
      beforeSequence: 5,
      maxBytes: 1,
      maxRows: 100,
      threadId: thread.id,
    });
    expect(window.rows.map((row) => row.sequence)).toEqual([2]);
  });
});

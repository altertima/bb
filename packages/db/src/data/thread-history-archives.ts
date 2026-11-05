import { and, eq, inArray, sql } from "drizzle-orm";
import type { ThreadEventType } from "@bb/domain";
import type { DbQueryConnection } from "../connection.js";
import {
  environments,
  events,
  retainedEventOutputs,
  threadHistoryArchives,
  type ThreadHistoryArchiveState,
} from "../schema.js";
import { queryInSqliteVariableBatches } from "./sqlite-variable-batches.js";

export const THREAD_HISTORY_ARCHIVE_FORMAT_VERSION = 1;

export const THREAD_HISTORY_KEEP_HOT_EVENT_TYPES = [
  "thread/identity",
  "thread/started",
  "thread/goal/updated",
  "thread/goal/cleared",
  "thread/extensionState/updated",
] as const satisfies readonly ThreadEventType[];

export type ThreadHistoryArchive = typeof threadHistoryArchives.$inferSelect;

export interface ThreadHistoryEventRow {
  createdAt: number;
  data: string;
  environmentId: string | null;
  id: string;
  itemId: string | null;
  itemKind: string | null;
  parentToolCallId: string | null;
  providerThreadId: string | null;
  scopeKind: string;
  sequence: number;
  threadId: string;
  turnId: string | null;
  type: string;
}

export interface ThreadHistoryRetainedOutputRow {
  eventId: string;
  expiresAt: number;
  outputPath: string;
  value: string;
}

export interface ThreadHistoryWindowArgs {
  afterSequence: number;
  beforeSequence: number;
  maxBytes: number;
  maxRows: number;
  threadId: string;
}

export interface ThreadHistoryColdWindow {
  bytes: number;
  lastSequence: number | null;
  retained: ThreadHistoryRetainedOutputRow[];
  rows: ThreadHistoryEventRow[];
}

export interface ThreadHistoryDeleteResult {
  bytes: number;
  deleted: number;
  lastSequence: number | null;
}

export interface ThreadHistoryCoolingCriteria {
  archivedBefore: number | null;
  idleBefore: number | null;
}

export interface ThreadHistoryArchiveTotals {
  coldThreads: number;
  packedBytes: number;
  rawBytes: number;
  rowCount: number;
}

const keepHotTypes = sql.join(
  THREAD_HISTORY_KEEP_HOT_EVENT_TYPES.map((type) => sql`${type}`),
  sql`, `,
);

function coolingEligibilitySql(
  criteria: ThreadHistoryCoolingCriteria,
  allowedArchiveStates: readonly ThreadHistoryArchiveState[],
) {
  const archivedRule =
    criteria.archivedBefore === null
      ? sql`0`
      : sql`(t.archived_at IS NOT NULL AND t.archived_at <= ${criteria.archivedBefore}
          AND (a.restored_at IS NULL OR a.restored_at <= ${criteria.archivedBefore}))`;
  const idleRule =
    criteria.idleBefore === null
      ? sql`0`
      : sql`(t.archived_at IS NULL AND t.updated_at <= ${criteria.idleBefore}
          AND (a.restored_at IS NULL OR a.restored_at <= ${criteria.idleBefore}))`;
  return sql`
    t.deleted_at IS NULL
    AND t.pinned_at IS NULL
    AND t.status NOT IN ('active', 'starting', 'stopping')
    AND (a.thread_id IS NULL OR a.state IN (${sql.join(
      allowedArchiveStates.map((state) => sql`${state}`),
      sql`, `,
    )}))
    AND (${archivedRule} OR ${idleRule})
    AND EXISTS (
      SELECT 1 FROM project_attachment_backfills b
      WHERE b.project_id = t.project_id AND b.phase = 'done'
    )
    AND NOT EXISTS (
      SELECT 1 FROM pending_interactions p
      WHERE p.thread_id = t.id AND p.status IN ('pending', 'resolving')
    )
    AND NOT EXISTS (SELECT 1 FROM queued_thread_messages q WHERE q.thread_id = t.id)
    AND NOT EXISTS (SELECT 1 FROM thread_pruning_work w WHERE w.thread_id = t.id)
    AND NOT EXISTS (
      SELECT 1 FROM events s
      WHERE s.thread_id = t.id
        AND s.item_kind = 'backgroundTask'
        AND s.type = 'item/started'
        AND s.item_id IS NOT NULL
        AND NOT EXISTS (
          SELECT 1 FROM events c
          WHERE c.thread_id = s.thread_id
            AND c.item_kind = 'backgroundTask'
            AND c.type = 'item/backgroundTask/completed'
            AND c.item_id = s.item_id
        )
    )`;
}

export function listThreadHistoryCoolingCandidates(
  db: DbQueryConnection,
  args: ThreadHistoryCoolingCriteria & { limit: number },
): string[] {
  if (args.archivedBefore === null && args.idleBefore === null) return [];
  return db
    .all<{ threadId: string }>(
      sql`
        SELECT t.id AS threadId
        FROM threads t
        LEFT JOIN thread_history_archives a ON a.thread_id = t.id
        WHERE ${coolingEligibilitySql(args, ["hot"])}
        ORDER BY coalesce(t.archived_at, t.updated_at), t.id
        LIMIT ${args.limit}
      `,
    )
    .map((row) => row.threadId);
}

export function isThreadHistoryCoolable(
  db: DbQueryConnection,
  args: ThreadHistoryCoolingCriteria & {
    inProgress?: boolean;
    threadId: string;
  },
): boolean {
  if (args.archivedBefore === null && args.idleBefore === null) return false;
  return (
    db.get<{ one: number }>(
      sql`
        SELECT 1 AS one
        FROM threads t
        LEFT JOIN thread_history_archives a ON a.thread_id = t.id
        WHERE t.id = ${args.threadId} AND ${coolingEligibilitySql(
          args,
          args.inProgress === true ? ["hot", "exporting"] : ["hot"],
        )}
      `,
    ) !== undefined
  );
}

export function getThreadHistoryArchive(
  db: DbQueryConnection,
  threadId: string,
): ThreadHistoryArchive | null {
  return (
    db
      .select()
      .from(threadHistoryArchives)
      .where(eq(threadHistoryArchives.threadId, threadId))
      .get() ?? null
  );
}

export function listThreadHistoryArchivesInStates(
  db: DbQueryConnection,
  states: readonly ThreadHistoryArchiveState[],
): ThreadHistoryArchive[] {
  return db
    .select()
    .from(threadHistoryArchives)
    .where(inArray(threadHistoryArchives.state, [...states]))
    .all();
}

export function listThreadHistoryArchiveThreadIds(
  db: DbQueryConnection,
): Set<string> {
  return new Set(
    db
      .select({ threadId: threadHistoryArchives.threadId })
      .from(threadHistoryArchives)
      .all()
      .map((row) => row.threadId),
  );
}

export function upsertThreadHistoryArchive(
  db: DbQueryConnection,
  archive: ThreadHistoryArchive,
): void {
  db.insert(threadHistoryArchives)
    .values(archive)
    .onConflictDoUpdate({
      target: threadHistoryArchives.threadId,
      set: {
        state: archive.state,
        formatVersion: archive.formatVersion,
        coldBeforeSequence: archive.coldBeforeSequence,
        cursorSequence: archive.cursorSequence,
        rowCount: archive.rowCount,
        rawBytes: archive.rawBytes,
        packedBytes: archive.packedBytes,
        coldAt: archive.coldAt,
        restoredAt: archive.restoredAt,
        updatedAt: archive.updatedAt,
      },
    })
    .run();
}

export function updateThreadHistoryArchiveInState(
  db: DbQueryConnection,
  args: {
    expectedState: ThreadHistoryArchiveState;
    patch: Partial<Omit<ThreadHistoryArchive, "threadId">>;
    threadId: string;
  },
): boolean {
  return (
    db
      .update(threadHistoryArchives)
      .set(args.patch)
      .where(
        and(
          eq(threadHistoryArchives.threadId, args.threadId),
          eq(threadHistoryArchives.state, args.expectedState),
        ),
      )
      .run().changes > 0
  );
}

export function deleteThreadHistoryArchive(
  db: DbQueryConnection,
  threadId: string,
): void {
  db.delete(threadHistoryArchives)
    .where(eq(threadHistoryArchives.threadId, threadId))
    .run();
}

export function getThreadHistoryArchiveTotals(
  db: DbQueryConnection,
): ThreadHistoryArchiveTotals {
  const row = db.get<ThreadHistoryArchiveTotals>(sql`
    SELECT
      count(*) AS coldThreads,
      coalesce(sum(packed_bytes), 0) AS packedBytes,
      coalesce(sum(raw_bytes), 0) AS rawBytes,
      coalesce(sum(row_count), 0) AS rowCount
    FROM thread_history_archives
    WHERE state IN ('cooling', 'cold', 'restoring')
  `);
  return row ?? { coldThreads: 0, packedBytes: 0, rawBytes: 0, rowCount: 0 };
}

const coldWindowSql = (args: ThreadHistoryWindowArgs) => sql`
  SELECT
    id,
    thread_id AS threadId,
    environment_id AS environmentId,
    scope_kind AS scopeKind,
    turn_id AS turnId,
    provider_thread_id AS providerThreadId,
    sequence,
    type,
    item_id AS itemId,
    item_kind AS itemKind,
    parent_tool_call_id AS parentToolCallId,
    data,
    created_at AS createdAt
  FROM events
  WHERE thread_id = ${args.threadId}
    AND sequence > ${args.afterSequence}
    AND sequence < ${args.beforeSequence}
    AND type NOT IN (${keepHotTypes})
  ORDER BY sequence
  LIMIT ${args.maxRows}
`;

function takeWithinBytes<T extends { data: string }>(
  rows: readonly T[],
  maxBytes: number,
): { bytes: number; rows: T[] } {
  const taken: T[] = [];
  let bytes = 0;
  for (const row of rows) {
    const rowBytes = Buffer.byteLength(row.data);
    if (taken.length > 0 && bytes + rowBytes > maxBytes) break;
    taken.push(row);
    bytes += rowBytes;
  }
  return { bytes, rows: taken };
}

function listRetainedOutputsForEvents(
  db: DbQueryConnection,
  eventIds: readonly string[],
): ThreadHistoryRetainedOutputRow[] {
  return queryInSqliteVariableBatches({
    dedupeKey: (eventId) => eventId,
    fixedVariableCount: 0,
    variableCountPerValue: 1,
    values: eventIds,
    queryBatch: (batch) =>
      db
        .select({
          eventId: retainedEventOutputs.eventId,
          expiresAt: retainedEventOutputs.expiresAt,
          outputPath: retainedEventOutputs.outputPath,
          value: retainedEventOutputs.value,
        })
        .from(retainedEventOutputs)
        .where(inArray(retainedEventOutputs.eventId, [...batch]))
        .all(),
  });
}

export function readThreadHistoryColdWindow(
  db: DbQueryConnection,
  args: ThreadHistoryWindowArgs,
): ThreadHistoryColdWindow {
  const { bytes, rows } = takeWithinBytes(
    db.all<ThreadHistoryEventRow>(coldWindowSql(args)),
    args.maxBytes,
  );
  return {
    bytes,
    lastSequence: rows.at(-1)?.sequence ?? null,
    retained: listRetainedOutputsForEvents(
      db,
      rows.map((row) => row.id),
    ),
    rows,
  };
}

export function deleteThreadHistoryColdWindow(
  db: DbQueryConnection,
  args: ThreadHistoryWindowArgs,
): ThreadHistoryDeleteResult {
  const candidates = db.all<{ data: string; id: string; sequence: number }>(
    sql`
      SELECT id, sequence, data FROM events
      WHERE thread_id = ${args.threadId}
        AND sequence > ${args.afterSequence}
        AND sequence < ${args.beforeSequence}
        AND type NOT IN (${keepHotTypes})
      ORDER BY sequence
      LIMIT ${args.maxRows}
    `,
  );
  const { bytes, rows } = takeWithinBytes(candidates, args.maxBytes);
  if (rows.length === 0) {
    return { bytes: 0, deleted: 0, lastSequence: null };
  }
  const ids = rows.map((row) => row.id);
  const deleted = db.delete(events).where(inArray(events.id, ids)).run()
    .changes;
  return { bytes, deleted, lastSequence: rows.at(-1)?.sequence ?? null };
}

export function countThreadHistoryColdRows(
  db: DbQueryConnection,
  args: { beforeSequence: number; threadId: string },
): number {
  return (
    db.get<{ value: number }>(sql`
      SELECT count(*) AS value FROM events
      WHERE thread_id = ${args.threadId}
        AND sequence < ${args.beforeSequence}
        AND type NOT IN (${keepHotTypes})
    `)?.value ?? 0
  );
}

export function insertThreadHistoryRows(
  db: DbQueryConnection,
  args: {
    now: number;
    retained: readonly ThreadHistoryRetainedOutputRow[];
    rows: readonly ThreadHistoryEventRow[];
  },
): number {
  const environmentIds = [
    ...new Set(
      args.rows.flatMap((row) =>
        row.environmentId === null ? [] : [row.environmentId],
      ),
    ),
  ];
  const liveEnvironmentIds = new Set(
    environmentIds.length === 0
      ? []
      : db
          .select({ id: environments.id })
          .from(environments)
          .where(inArray(environments.id, environmentIds))
          .all()
          .map((row) => row.id),
  );
  let inserted = 0;
  for (const row of args.rows) {
    const environmentId =
      row.environmentId !== null && liveEnvironmentIds.has(row.environmentId)
        ? row.environmentId
        : null;
    inserted += db.run(sql`
      INSERT OR IGNORE INTO events (
        id, thread_id, environment_id, scope_kind, turn_id, provider_thread_id,
        sequence, type, item_id, item_kind, parent_tool_call_id, data, created_at
      ) VALUES (
        ${row.id}, ${row.threadId}, ${environmentId}, ${row.scopeKind}, ${row.turnId},
        ${row.providerThreadId}, ${row.sequence}, ${row.type}, ${row.itemId},
        ${row.itemKind}, ${row.parentToolCallId}, ${row.data}, ${row.createdAt}
      )
    `).changes;
  }
  for (const output of args.retained) {
    if (output.expiresAt <= args.now) continue;
    db.run(sql`
      INSERT OR IGNORE INTO retained_event_outputs (event_id, output_path, value, expires_at)
      SELECT ${output.eventId}, ${output.outputPath}, ${output.value}, ${output.expiresAt}
      WHERE EXISTS (SELECT 1 FROM events WHERE id = ${output.eventId})
    `);
  }
  return inserted;
}

export function hasRunningThreads(db: DbQueryConnection): boolean {
  return (
    db.get<{ one: number }>(sql`
      SELECT 1 AS one FROM threads
      WHERE status IN ('active', 'starting') AND deleted_at IS NULL
      LIMIT 1
    `) !== undefined
  );
}

export function deleteThreadPruningCursorsForThread(
  db: DbQueryConnection,
  threadId: string,
): void {
  db.run(sql`DELETE FROM thread_pruning_cursors WHERE scope = ${threadId}`);
}

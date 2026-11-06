import { createHash } from "node:crypto";
import { defineRpcContract } from "@get-bb/plugin-sdk";
import { z } from "zod";
import type { WorkflowRunRow } from "./data.js";
import {
  parseStoredWorkflowSettings,
  type WorkflowSettings,
} from "./settings.js";

const admissionArgsSchema = z.object({
  sourceCommit: z.string().regex(/^[0-9a-f]{40}$/),
  branch: z.string().min(1).max(256),
  profile: z.object({
    name: z.string().min(1).max(128),
    maxRepairs: z.number(),
    maxConcurrentAgents: z.number(),
    maxAgentCalls: z.number(),
    maxRunMinutes: z.number(),
    roles: z.record(
      z.string().min(1).max(128),
      z.object({
        provider: z.string().max(128),
        model: z.string().max(256),
        reasoningLevel: z.string().max(128),
      }),
    ),
  }),
});
function admissionArgs(
  text: string,
): z.infer<typeof admissionArgsSchema> | null {
  const result = admissionArgsSchema.safeParse(JSON.parse(text));
  return result.success ? result.data : null;
}

const runSchema = z.object({
  id: z.string().min(1),
  projectId: z.string().min(1),
  originThreadId: z.string().min(1),
  environmentId: z.string().min(1),
  name: z.string(),
  sourceHash: z.string(),
  args: admissionArgsSchema.nullable(),
  settings: z.record(z.string(), z.number()),
  status: z.enum(["queued", "running"]),
  createdAt: z.number(),
  startedAt: z.number().nullable(),
});
export const workflowInventorySchema = z.object({
  version: z.literal(1),
  scope: z.literal("server"),
  complete: z.literal(true),
  settings: z.record(z.string(), z.number()),
  token: z.string().regex(/^[0-9a-f]{64}$/),
  runs: z.array(runSchema),
});
export type WorkflowInventory = z.infer<typeof workflowInventorySchema>;
export const workflowInventoryContract = defineRpcContract({
  "workflows.v1.inventory": {
    input: z.null(),
    output: workflowInventorySchema,
    experimental_description:
      "Complete server-wide queued/running workflow identities, non-secret admission metadata and stored settings for trusted server operators and plugins. Token supports conditional workflow admission; not a reservation of threads or environments.",
  },
});

export function buildWorkflowInventory(
  rows: WorkflowRunRow[],
  settings: WorkflowSettings,
): WorkflowInventory {
  const runs = rows.map((run) => ({
    id: run.id,
    projectId: run.projectId,
    originThreadId: run.originThreadId,
    environmentId: run.environmentId,
    name: run.name,
    sourceHash: run.sourceHash,
    args: admissionArgs(run.argsJson),
    settings: parseStoredWorkflowSettings(JSON.parse(run.settingsJson)),
    status: run.status,
    createdAt: run.createdAt,
    startedAt: run.startedAt,
  }));
  const token = createHash("sha256")
    .update(JSON.stringify({ runs, settings }))
    .digest("hex");
  return workflowInventorySchema.parse({
    version: 1,
    scope: "server",
    complete: true,
    settings,
    token,
    runs,
  });
}

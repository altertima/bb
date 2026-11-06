import Database from "better-sqlite3";
import {
  createFakePluginHost,
  makeThreadResponse,
} from "@get-bb/plugin-sdk/testing";
import { describe, expect, it } from "vitest";
import { createRun, listActiveRuns, migrations, settleRun } from "./data.js";
import { buildWorkflowInventory } from "./inventory.js";
import { DEFAULT_WORKFLOW_SETTINGS } from "./settings.js";
import { createWorkflowService } from "./service.js";
import plugin from "./server.js";

const source =
  'export const meta = { name: "inventory-test", description: "Inventory test" }; return null;';

describe("complete workflow inventory", () => {
  it("includes old active runs across projects beyond 50 newer terminal runs", () => {
    const db = new Database(":memory:");
    try {
      db.exec(migrations.join("\n"));
      const add = (projectId: string) =>
        createRun(db, {
          projectId,
          originThreadId: `origin-${projectId}`,
          environmentId: `env-${projectId}`,
          originProvider: "pi",
          originModel: "test",
          originReasoningLevel: "high",
          originPermissionMode: "full",
          name: "inventory-test",
          source,
          sourceHash: "hash",
          argsJson: JSON.stringify({
            sourceCommit: "a".repeat(40),
            branch: "task/example",
            secret: "NEVER-EXPOSE",
            specText: "PRIVATE-SPEC",
            profile: {
              name: "fullstack-quality",
              maxRepairs: 2,
              maxConcurrentAgents: 1,
              maxAgentCalls: 11,
              maxRunMinutes: 90,
              roles: Object.fromEntries(
                ["planner", "backend", "frontend", "qa", "reviewer"].map(
                  (role) => [
                    role,
                    { provider: "pi", model: "test", reasoningLevel: "high" },
                  ],
                ),
              ),
            },
          }),
          settingsJson: JSON.stringify(DEFAULT_WORKFLOW_SETTINGS),
          resumedFromRunId: null,
        });
      const first = add("first"),
        second = add("second");
      db.prepare(
        "UPDATE workflow_runs SET status = 'running' WHERE id = ?",
      ).run(first.id);
      for (let n = 0; n < 75; n++) {
        settleRun(db, {
          id: add("first").id,
          status: "succeeded",
          result: null,
          error: null,
        });
      }
      const inventory = buildWorkflowInventory(
        listActiveRuns(db),
        DEFAULT_WORKFLOW_SETTINGS,
      );
      expect(inventory).toMatchObject({
        version: 1,
        scope: "server",
        complete: true,
      });
      expect(inventory.runs.map((run) => run.id).sort()).toEqual(
        [first.id, second.id].sort(),
      );
      expect(inventory.runs[0]).toMatchObject({
        sourceHash: "hash",
        args: { sourceCommit: "a".repeat(40), branch: "task/example" },
        settings: DEFAULT_WORKFLOW_SETTINGS,
      });
      expect(JSON.stringify(inventory)).not.toContain('"source":');
      expect(JSON.stringify(inventory)).not.toContain("NEVER-EXPOSE");
      expect(JSON.stringify(inventory)).not.toContain("PRIVATE-SPEC");
      db.prepare("UPDATE workflow_runs SET args_json = '{' WHERE id = ?").run(
        first.id,
      );
      expect(() =>
        buildWorkflowInventory(listActiveRuns(db), DEFAULT_WORKFLOW_SETTINGS),
      ).toThrow(`invalid args for ${first.id}`);
      db.prepare(
        "UPDATE workflow_runs SET args_json = 'null', settings_json = '{' WHERE id = ?",
      ).run(first.id);
      expect(() =>
        buildWorkflowInventory(listActiveRuns(db), DEFAULT_WORKFLOW_SETTINGS),
      ).toThrow(`invalid settings for ${first.id}`);
    } finally {
      db.close();
    }
  });

  it("rejects a concurrent start using a stale inventory token and a third fresh start", async () => {
    const { bb, harness } = createFakePluginHost({
      pluginId: "workflows",
      sdk: {
        threads: {
          get: async ({ threadId }) =>
            makeThreadResponse({
              id: threadId,
              environmentId: `env-${threadId}`,
            }),
          defaultExecutionOptions: async () => ({
            model: "test",
            reasoningLevel: "high",
            permissionMode: "full",
            serviceTier: "default",
            source: "default",
          }),
        },
      },
    });
    try {
      const db = bb.storage.database();
      bb.storage.migrate(db, migrations);
      const service = createWorkflowService(bb, db, {
        ...DEFAULT_WORKFLOW_SETTINGS,
        maxActiveRuns: 2,
      });
      const start = (originThreadId: string, inventoryToken: string) =>
        service.start({
          projectId: originThreadId,
          originThreadId,
          source,
          args: null,
          resumedFromRunId: null,
          inventoryToken,
        });
      const token = service.inventory().token;
      const raced = await Promise.allSettled([
        start("one", token),
        start("two", token),
      ]);
      expect(raced.filter((r) => r.status === "fulfilled")).toHaveLength(1);
      expect(service.inventory().runs).toHaveLength(1);
      const nextToken = service.inventory().token;
      const oneSlotRace = await Promise.allSettled([
        start("two", nextToken),
        start("other", nextToken),
      ]);
      expect(oneSlotRace.filter((r) => r.status === "fulfilled")).toHaveLength(
        1,
      );
      expect(service.inventory().runs).toHaveLength(2);
      await expect(start("three", service.inventory().token)).rejects.toThrow(
        "capacity unavailable",
      );
      expect(service.inventory().runs).toHaveLength(2);
      service.updateSettings({
        ...DEFAULT_WORKFLOW_SETTINGS,
        maxActiveRuns: 3,
      });
      await expect(start("three", token)).rejects.toThrow("inventory changed");
    } finally {
      await harness.dispose();
    }
  });

  it("exposes the same complete inventory via CLI and discoverable RPC without thread context", async () => {
    const { bb, harness } = createFakePluginHost({ pluginId: "workflows" });
    try {
      await plugin(bb);
      const result = await harness.runCli(["inventory", "--json"]);
      expect(result.exitCode).toBe(0);
      const rpc = await harness.callRpc("workflows.v1.inventory", null);
      expect(JSON.parse(result.stdout)).toEqual(rpc);
      expect(
        harness.registrations.experimental_publishedRpcMethods,
      ).toContainEqual(
        expect.objectContaining({ method: "workflows.v1.inventory" }),
      );
    } finally {
      await harness.dispose();
    }
  });
});

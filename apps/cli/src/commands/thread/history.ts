import { Command } from "commander";
import type { ThreadHistoryResponse } from "@bb/server-contract";
import { action } from "../../action.js";
import { createCliBbSdk } from "../../client.js";
import { outputJson, requireThreadIdOrSelf } from "../helpers.js";

interface ThreadHistoryCommandOptions {
  json?: boolean;
  self?: boolean;
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

function describeHistory(history: ThreadHistoryResponse): string {
  if (history.storage === "hot") {
    return `Thread ${history.threadId}: history is in the database`;
  }
  return `Thread ${history.threadId}: history is in the archive (${history.archivedRows.toLocaleString("en-US")} events, ${formatBytes(history.packedBytes)})`;
}

export function registerHistoryCommands(
  parent: Command,
  getUrl: () => string,
): void {
  const history = parent
    .command("history-archive")
    .description(
      "Show or move a thread's event history between the database and the archive",
    );

  const subcommands = [
    {
      name: "status",
      description: "Show whether the thread's history is in the database or the archive",
      run: (threadId: string) =>
        createCliBbSdk(getUrl()).threads.experimental_history({ threadId }),
    },
    {
      name: "cool",
      description:
        "Move the thread's history to the archive now instead of waiting for the archive delay",
      run: (threadId: string) =>
        createCliBbSdk(getUrl()).threads.experimental_coolHistory({ threadId }),
    },
    {
      name: "restore",
      description: "Restore the thread's archived history into the database now",
      run: (threadId: string) =>
        createCliBbSdk(getUrl()).threads.experimental_restoreHistory({
          threadId,
        }),
    },
  ] as const;

  for (const subcommand of subcommands) {
    history
      .command(`${subcommand.name} [id]`)
      .description(subcommand.description)
      .option("--self", "Target the current thread (from BB_THREAD_ID)")
      .option("--json", "Print machine-readable JSON output")
      .action(
        action(
          async (
            id: string | undefined,
            opts: ThreadHistoryCommandOptions,
          ) => {
            const threadId = requireThreadIdOrSelf(id, opts);
            const result = await subcommand.run(threadId);
            if (outputJson(opts, result)) return;
            console.log(describeHistory(result));
          },
        ),
      );
  }
}

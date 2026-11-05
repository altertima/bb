import type { Hono } from "hono";
import {
  publicApiRoutes,
  typedRoutes,
  type PublicApiSchema,
} from "@bb/server-contract";
import type { AppDeps } from "../../types.js";
import { ApiError } from "../../errors.js";
import { requirePublicThread } from "../../services/lib/entity-lookup.js";
import {
  ensureThreadHistoryHot,
  threadHistoryStatus,
} from "../../services/threads/history-archive/thread-history.js";
import { getThreadHistoryMaintenance } from "../../services/threads/history-archive/thread-history-cooling.js";

export function registerThreadHistoryRoutes(app: Hono, deps: AppDeps): void {
  const { get, post } = typedRoutes<PublicApiSchema>(app, {
    onValidationError: (msg) => new ApiError(400, "invalid_request", msg),
  });
  const routes = publicApiRoutes.threads;

  get(routes.history, (context) => {
    const thread = requirePublicThread(deps.db, context.req.param("id"));
    return context.json(threadHistoryStatus(deps, thread.id));
  });

  post(routes.coolHistory, async (context) => {
    const thread = requirePublicThread(deps.db, context.req.param("id"));
    const maintenance = getThreadHistoryMaintenance(deps.db);
    if (maintenance === null) {
      throw new ApiError(
        503,
        "history_archiving_unavailable",
        "Thread history archiving is not running on this server",
      );
    }
    if (threadHistoryStatus(deps, thread.id).storage === "hot") {
      try {
        await maintenance.coolNow(thread.id);
      } catch (error) {
        throw new ApiError(
          409,
          "invalid_request",
          error instanceof Error ? error.message : "History can't be archived now",
        );
      }
    }
    return context.json(threadHistoryStatus(deps, thread.id));
  });

  post(routes.restoreHistory, async (context) => {
    const thread = requirePublicThread(deps.db, context.req.param("id"));
    await ensureThreadHistoryHot(deps, thread.id);
    return context.json(threadHistoryStatus(deps, thread.id));
  });
}

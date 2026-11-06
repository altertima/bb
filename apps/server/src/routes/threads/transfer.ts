import { createReadStream, createWriteStream } from "node:fs";
import { mkdtemp, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import type { ReadableStream as NodeReadableStream } from "node:stream/web";
import type { Hono } from "hono";
import { getExperiments } from "@bb/db";
import {
  publicApiRoutes,
  typedRoutes,
  type PublicApiSchema,
} from "@bb/server-contract";
import type { AppDeps } from "../../types.js";
import { ApiError } from "../../errors.js";
import { requirePublicThread } from "../../services/lib/entity-lookup.js";
import {
  exportThreadFile,
  importThreadFile,
} from "../../services/threads/history-archive/thread-transfer.js";
import { readMultipartFormData } from "../multipart-form-data.js";

const THREAD_FILE_CONTENT_TYPE = "application/zstd";

const COLD_HISTORY_EXPERIMENT_DISABLED_MESSAGE =
  'Thread export and import are off. Turn on the "Cold thread history" experiment in Settings → Experiments, or run bb settings experiment coldHistory true, then try again.';

function assertColdHistoryExperimentEnabled(deps: AppDeps): void {
  if (!getExperiments(deps.db).coldHistory) {
    throw new ApiError(
      403,
      "cold_history_experiment_disabled",
      COLD_HISTORY_EXPERIMENT_DISABLED_MESSAGE,
    );
  }
}

export function registerThreadTransferRoutes(app: Hono, deps: AppDeps): void {
  const { get, post } = typedRoutes<PublicApiSchema>(app, {
    onValidationError: (msg) => new ApiError(400, "invalid_request", msg),
  });
  const routes = publicApiRoutes.threads;

  const makeWorkDir = () => mkdtemp(join(tmpdir(), "bb-thread-file-"));
  const removeWorkDir = (workDir: string) =>
    rm(workDir, { force: true, recursive: true }).catch((error: unknown) => {
      deps.logger.warn(
        { err: error, workDir },
        "Thread file transfer could not remove its work directory",
      );
    });

  get(routes.exportThread, async (context) => {
    assertColdHistoryExperimentEnabled(deps);
    const thread = requirePublicThread(deps.db, context.req.param("id"));
    const fileName = `${thread.id}.jsonl.zst`;
    const workDir = await makeWorkDir();
    const path = join(workDir, fileName);
    try {
      await exportThreadFile(deps, { path, threadId: thread.id });
    } catch (error) {
      await removeWorkDir(workDir);
      throw error;
    }
    const { size } = await stat(path);
    const stream = createReadStream(path);
    stream.once("close", () => {
      void removeWorkDir(workDir);
    });
    return new Response(Readable.toWeb(stream) as ReadableStream<Uint8Array>, {
      status: 200,
      headers: {
        "cache-control": "no-store",
        "content-disposition": `attachment; filename="${fileName}"`,
        "content-length": String(size),
        "content-type": THREAD_FILE_CONTENT_TYPE,
      },
    });
  });

  post(routes.importThread, async (context) => {
    assertColdHistoryExperimentEnabled(deps);
    const formData = await readMultipartFormData(context.req);
    const fields = [...formData.keys()];
    const file = formData.get("file");
    if (fields.length !== 1 || !(file instanceof File)) {
      throw new ApiError(
        400,
        "invalid_request",
        'Thread import accepts exactly one multipart field named "file"',
      );
    }
    const workDir = await makeWorkDir();
    try {
      const path = join(workDir, "import.jsonl.zst");
      await pipeline(
        Readable.fromWeb(file.stream() as NodeReadableStream<Uint8Array>),
        createWriteStream(path),
      );
      return context.json(await importThreadFile(deps, path));
    } finally {
      await removeWorkDir(workDir);
    }
  });
}

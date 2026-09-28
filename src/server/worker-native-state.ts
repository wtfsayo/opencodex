// What a Cloudflare container hands its Worker at startup so the Worker can serve turns as this
// process would. Every step is a no-op off Cloudflare.
import { CLAUDE_CODE_HEADERS, CLAUDE_CODE_RUNTIME_HEADERS } from "../adapters/client-fingerprint";
import { durableMirrorEnabled, stateRequest } from "../lib/durable-mirror";
import { startWorkerUsageInbox } from "../usage/worker-usage-inbox";

// A Durable Object reset or a lease that moved drops what was published; publishing again on this
// cadence restores it. A failed first publish is retried sooner.
const REPUBLISH_MS = 5 * 60 * 1000;
const FIRST_RETRY_MS = 10_000;
let republish: ReturnType<typeof setInterval> | undefined;

/**
 * The Claude Code fingerprint headers that name this process's runtime. A Claude subscription turn
 * the Worker serves sends these, not the Worker runtime's, so the upstream sees the same headers.
 */
export function publishClientRuntimeForWorker(retrySoon = false): Promise<void> | undefined {
  // Only where the Worker serves requests (the Worker sets it; see containerEnv).
  if (process.env.OCX_WORKER_NATIVE_STATE !== "1" || !durableMirrorEnabled()) return undefined;
  const headers = Object.fromEntries(CLAUDE_CODE_RUNTIME_HEADERS.map(name => [name, CLAUDE_CODE_HEADERS[name]!]));
  return stateRequest("/client-runtime", {
    method: "PUT",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(headers),
  })?.then(response => { void response.body?.cancel(); return response.ok; }, () => false)
    .then(ok => { if (!ok && retrySoon) setTimeout(() => void publishClientRuntimeForWorker(), FIRST_RETRY_MS).unref?.(); });
}

export function startWorkerNativeState(): void {
  startWorkerUsageInbox();
  if (publishClientRuntimeForWorker(true) && !republish) {
    republish = setInterval(() => void publishClientRuntimeForWorker(), REPUBLISH_MS);
    republish.unref?.();
  }
}

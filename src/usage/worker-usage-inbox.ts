// Usage rows for turns the Cloudflare Worker answered itself (src/server/cloudflare-native-chat.ts)
// wait in the hub's Durable Object, because the Worker has no usage log. On a Cloudflare deployment
// this process drains them into usage.jsonl at startup and then once a minute. Elsewhere nothing runs.
//
// Delivery is at least once: a crash between appending a batch and acknowledging it appends that
// batch again on the next drain.
import { durableMirrorEnabled, stateRequest } from "../lib/durable-mirror";
import { appendUsageEntry, type PersistedUsageEntry } from "./log";

const DRAIN_INTERVAL_MS = 60_000;
const BATCH = 500;
// A drain never runs longer than this many batches, so a large backlog cannot hold a tick forever.
const MAX_BATCHES = 40;
let timer: ReturnType<typeof setInterval> | undefined;
let draining: Promise<number> | undefined;

type QueuedRow = { seq: number; row: unknown };

function toEntry(row: unknown): PersistedUsageEntry | null {
  if (!row || typeof row !== "object" || Array.isArray(row)) return null;
  const value = row as Record<string, unknown>;
  const str = (key: string) => typeof value[key] === "string" && (value[key] as string).length > 0;
  const num = (key: string) => typeof value[key] === "number" && Number.isFinite(value[key] as number);
  if (!str("requestId") || !str("provider") || !str("model") || !num("timestamp") || !num("status") || !num("durationMs")) return null;
  if (value.usageStatus !== "reported" && value.usageStatus !== "unreported") return null;
  return row as PersistedUsageEntry;
}

/** Appends every queued row it can take, acknowledging each batch; returns how many were appended. */
export function drainWorkerUsageInbox(): Promise<number> {
  draining ??= (async () => {
    let appended = 0;
    for (let batch = 0; batch < MAX_BATCHES; batch++) {
      const response = await stateRequest(`/usage-inbox?limit=${BATCH}`);
      if (!response?.ok) break;
      const { rows } = await response.json() as { rows?: QueuedRow[] };
      if (!Array.isArray(rows) || rows.length === 0) break;
      for (const { row } of rows) {
        const entry = toEntry(row);
        if (!entry) continue;
        appendUsageEntry(entry);
        appended++;
      }
      const ack = await stateRequest("/usage-inbox/ack", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ seqs: rows.map(item => item.seq) }),
      });
      if (!ack?.ok || rows.length < BATCH) break;
    }
    return appended;
  })().catch(error => {
    console.warn(`[usage] Worker usage rows not drained: ${error instanceof Error ? error.message : String(error)}`);
    return 0;
  }).finally(() => { draining = undefined; });
  return draining;
}

/** Starts draining on a Cloudflare deployment; a no-op anywhere else. */
export function startWorkerUsageInbox(): void {
  if (timer || !durableMirrorEnabled()) return;
  void drainWorkerUsageInbox();
  timer = setInterval(() => void drainWorkerUsageInbox(), DRAIN_INTERVAL_MS);
  timer.unref?.();
}

export function stopWorkerUsageInboxForTests(): void {
  if (timer) clearInterval(timer);
  timer = undefined;
}

// Usage rows for turns the Cloudflare Worker answered itself (src/server/cloudflare-native-chat.ts)
// wait in the hub's Durable Object, because the Worker has no usage log. On a Cloudflare deployment
// this process drains them into usage.jsonl at startup and then once a minute. Elsewhere nothing runs.
//
// Delivery is at least once: a crash between appending a batch and acknowledging it appends that
// batch again on the next drain.
import { durableMirrorEnabled, stateRequest } from "../lib/durable-mirror";
import { appendUsageEntry, isKnownUsageSurface, type PersistedUsageEntry } from "./log";
import { KEY_ACCOUNT_LOG_LABEL_RE } from "../codex/account-label";
import { sharedSpendLedger } from "../lib/spend-reservation-ledger";

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
  // Projected, not passed through: usage.jsonl holds request metadata and token counts, and that
  // must not depend on what the Worker happened to put in the row.
  const entry: Record<string, unknown> = {};
  for (const key of ENTRY_FIELDS) if (value[key] !== undefined) entry[key] = value[key];
  // Checked against the shapes ocx itself writes, since usage summaries group by them.
  if (isKnownUsageSurface(value.surface)) entry.surface = value.surface;
  if (typeof value.accountLogLabel === "string" && KEY_ACCOUNT_LOG_LABEL_RE.test(value.accountLogLabel)) entry.accountLogLabel = value.accountLogLabel;
  if (typeof value.conversationId === "string" && /^[0-9a-f]{32}$/.test(value.conversationId)) entry.conversationId = value.conversationId;
  for (const key of ["resolvedModel", "wireModel", "requestedEffort"] as const) {
    if (typeof value[key] === "string" && (value[key] as string).length > 0 && (value[key] as string).length <= 200) entry[key] = value[key];
  }
  if (value.usage && typeof value.usage === "object" && !Array.isArray(value.usage)) {
    const usage: Record<string, number> = {};
    for (const [key, count] of Object.entries(value.usage as Record<string, unknown>)) {
      if (typeof count === "number" && Number.isFinite(count) && key.endsWith("Tokens")) usage[key] = count;
    }
    entry.usage = usage;
  }
  return entry as unknown as PersistedUsageEntry;
}

// WorkerUsageRow (src/server/cloudflare-native-chat-api.ts), minus usage, which is projected above.
const ENTRY_FIELDS = [
  "requestId", "timestamp", "provider", "model", "requestedModel", "inboundProtocol", "admissionKind",
  "status", "durationMs", "firstOutputMs", "usageStatus", "totalTokens",
] as const;

/**
 * request-spend.ts books every send in the spend ledger, limits or not, so a ceiling configured
 * later counts what was already spent. A Worker turn is booked here, as one send already made:
 * settled with its usage, or unresolved without one. Keyed by the row's request id, so a batch
 * appended twice is refused as a duplicate send rather than counted twice.
 */
function bookWorkerSpend(entry: PersistedUsageEntry): void {
  try {
    const ledger = sharedSpendLedger();
    const sendId = `worker:${entry.requestId}`;
    const decision = ledger.reserve({
      sendId,
      scopes: {
        ...(entry.accountLogLabel !== undefined ? { identityId: entry.accountLogLabel } : {}),
        poolId: entry.provider,
      },
      inputTokens: entry.usage?.inputTokens ?? 0,
      outputCeilingTokens: 0,
      alreadySent: true,
    });
    if (!decision.reserved) return;
    ledger.markDispatched(sendId);
    if (entry.usage) ledger.settle(sendId, { inputTokens: entry.usage.inputTokens, outputTokens: entry.usage.outputTokens });
    else ledger.markLost(sendId);
  } catch (error) {
    console.warn(`[usage] Worker turn not booked in the spend ledger: ${error instanceof Error ? error.name : "error"}`);
  }
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
        bookWorkerSpend(entry);
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

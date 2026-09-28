// The pure half of the local previous_response_id replay (state.ts): what an entry holds once a
// response completes, and how a later request's input is built from it. state.ts keeps entries
// across turns on disk; the Cloudflare Worker keeps one socket's in memory, with the same rules.
import { clientCarriedPrefixLength, providerIssuedIdentity } from "./replay-fingerprint";

export type ReplayEntryItems = {
  clientThreadId?: string;
  items: unknown[];
  /** Where the response's output begins in `items`. */
  providerOutputStart?: number;
};

export function inputItems(input: unknown): unknown[] {
  if (input === undefined) return [];
  if (Array.isArray(input)) return input;
  if (typeof input === "string") return [{ role: "user", content: input }];
  return [input];
}

export function normalizedClientThreadId(value: unknown): string | undefined {
  return typeof value === "string" && value.trim().length > 0 ? value.trim() : undefined;
}

/**
 * The entry a completed response leaves, or undefined for one rememberResponseState does not keep:
 * no id or output, or a status other than completed (an incomplete one only at max_output_tokens).
 */
export function replayEntryFor(
  request: Record<string, unknown>,
  response: { id?: unknown; output?: unknown; status?: unknown; incomplete_details?: unknown },
  clientThreadId?: string,
): (ReplayEntryItems & { id: string }) | undefined {
  if (typeof response.id !== "string" || !Array.isArray(response.output)) return undefined;
  if (response.status === "incomplete") {
    const details = response.incomplete_details;
    if (!details || typeof details !== "object" || Array.isArray(details)
      || (details as { reason?: unknown }).reason !== "max_output_tokens") return undefined;
  } else if (response.status !== undefined && response.status !== "completed") return undefined;
  const thread = normalizedClientThreadId(clientThreadId);
  const requestItems = inputItems(request.input);
  return {
    id: response.id,
    ...(thread ? { clientThreadId: thread } : {}),
    items: [...requestItems, ...response.output],
    providerOutputStart: requestItems.length,
  };
}

export type ReplayExpansion =
  | { kind: "scope-mismatch" }
  /** The client already carried the whole entry: its input is kept as sent. */
  | { kind: "carried"; body: Record<string, unknown>; prefixLength: number }
  | { kind: "expanded"; body: Record<string, unknown>; prefixLength: number };

/** expandPreviousResponseInput once the entry is found and materialized. */
export function expandWithReplayEntry(
  request: Record<string, unknown>,
  entry: ReplayEntryItems,
  clientThreadId?: string,
): ReplayExpansion {
  // A Codex task must never inherit another task's continuation, nor a legacy unscoped entry.
  // Unscoped callers retain backward-compatible replay only with other unscoped entries.
  if (normalizedClientThreadId(clientThreadId) !== normalizedClientThreadId(entry.clientThreadId)) return { kind: "scope-mismatch" };
  // The client already replayed this history verbatim. Prepending the stored copy would
  // double it, and the doubled turn is stored again, so the next turn triples (#1412 saw
  // 127k of real context reach 1.3M tokens this way).
  //
  // Three conditions, all required. The run must cover the whole stored entry; it must reach
  // the provider-output region; and some matched item in that region must carry a
  // provider-issued id. The last one is the load-bearing part: content equality alone proves
  // two items look alike, not that they are the same occurrence, so a client that merely
  // repeats its own message would otherwise authorize a skip that deletes real history.
  // There is no invariant that provider output always carries ids, so an entry whose output
  // has none simply never skips.
  const clientInput = inputItems(request.input);
  const stored = entry.items;
  const anchor = entry.providerOutputStart;
  const carried = clientCarriedPrefixLength(stored, clientInput);
  if (
    carried === stored.length
    && anchor !== undefined
    && carried > anchor
    && stored.slice(anchor, carried).some(item => providerIssuedIdentity(item) !== null)
  ) {
    // Keep previous_response_id: Kiro and Cursor recover their conversation ids from it
    // (kiro-wire.ts, cursor/request-builder.ts). Only the concatenation is skipped.
    return { kind: "carried", body: { ...request }, prefixLength: carried };
  }
  return { kind: "expanded", body: { ...request, input: [...stored, ...clientInput] }, prefixLength: stored.length };
}

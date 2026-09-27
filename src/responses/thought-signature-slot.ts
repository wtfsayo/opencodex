// Where thought signatures are remembered for replay. Import-free of storage, so the request
// parser and the SSE bridge can run where there is no disk (the Cloudflare Worker): until a store
// registers, nothing is remembered and nothing is found, which costs only signature replay.
// thought-signature-replay.ts registers the proxy's disk-backed store when it loads.
import type { OcxProviderOpaqueToolCallMetadata, OcxReasoningReplayScopeRef } from "../types";
import { responsesExtraContentFromProviderMetadata } from "./provider-opaque-metadata";

export interface ThoughtSignatureStore {
  remember(callId: string, signature: string, scope: OcxReasoningReplayScopeRef | undefined): { durable: Promise<void> };
  lookup(callId: string, scope: OcxReasoningReplayScopeRef | undefined): string | undefined;
  awaitDurability(capMs?: number): Promise<void>;
}

const NO_STORE: ThoughtSignatureStore = {
  remember: () => ({ durable: Promise.resolve() }),
  lookup: () => undefined,
  awaitDurability: () => Promise.resolve(),
};
let store = NO_STORE;

export function registerThoughtSignatureStore(next: ThoughtSignatureStore): void {
  store = next;
}

/** Look up a signature previously handed out for this call in THIS scope, if still fresh. */
export function lookupReplayThoughtSignature(
  callId: string,
  scope: OcxReasoningReplayScopeRef | undefined,
): string | undefined {
  return store.lookup(callId, scope);
}

/**
 * Bounded best-effort barrier for the store's pending writes; see the disk store's
 * awaitStoreDurability for why the turn waits at all.
 */
export function awaitThoughtSignatureDurability(capMs = 250): Promise<void> {
  return store.awaitDurability(capMs);
}

/**
 * Serialize provider metadata onto an outbound Responses function_call item AND remember the
 * signature server-side, so a client that replays the call without echoing extra_content can
 * still be served from the store.
 */
export function rememberAndSerializeExtraContent(
  callId: string,
  metadata: OcxProviderOpaqueToolCallMetadata | undefined,
  scope: OcxReasoningReplayScopeRef | undefined,
): {
  extra?: { extra_content: { google: { thought_signature: string } } };
  durable: Promise<void>;
} {
  const extra = responsesExtraContentFromProviderMetadata(metadata);
  if (!extra) return { durable: Promise.resolve() };
  const { durable } = store.remember(
    callId,
    extra.extra_content.google.thought_signature,
    scope,
  );
  return { extra, durable };
}

/**
 * Remember the signature without serializing it onto the item. Used for freeform tools, whose
 * Responses items are custom_tool_call blocks that cannot carry extra_content — the signature
 * still must be stored so the replayed call (which comes back as custom_tool_call and never
 * echoes metadata) can be re-signed server-side.
 */
export function rememberExtraContentForReplay(
  callId: string,
  metadata: OcxProviderOpaqueToolCallMetadata | undefined,
  scope: OcxReasoningReplayScopeRef | undefined,
): Promise<void> {
  const extra = responsesExtraContentFromProviderMetadata(metadata);
  if (!extra) return Promise.resolve();
  return store.remember(
    callId,
    extra.extra_content.google.thought_signature,
    scope,
  ).durable;
}

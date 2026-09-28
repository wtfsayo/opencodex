// On a Cloudflare deployment whose Worker replays model lists (OCX_WORKER_NATIVE_STATE, set by the
// Worker), hands each GET /v1/models answer to the Durable Object so the Worker can give it without
// starting the container (deploy/cloudflare/src/index.ts). An answer is published only while it is
// the one this process would give again: until the first moment it would refetch any input, and
// while auth, Codex accounts and config are unchanged (see LeaseState.modelListRead).
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { getConfigDir } from "../config/paths";
import { DEFAULT_MODEL_CACHE_TTL_MS, providerModelsFreshUntil } from "../codex/model-cache";
import { shouldIncludeAccountBoundNativeOpenAi, shouldIncludeNativeOpenAi } from "../codex/catalog/metadata";
import { providerSupportsLiveModelDiscovery } from "../providers/static-model-discovery";
import {
  DURABLE_DOCUMENT_FILES, documentDigest, durableMirrorEnabled, readSequenceState, sequenceFileFor, stateRequest,
  type DurableDocumentName,
} from "../lib/durable-mirror";
import type { OcxConfig } from "../types";
import type { DataPlaneAdmission } from "./auth-cors";
import { modelListReplayInputs, modelListReplayKey } from "./model-list-replay-key";

// The headers of an answer the Worker may give back; the rest belong to this hop.
const REPLAYED_HEADERS = new Set(["content-type", "cache-control", "vary", "x-content-type-options"]);
let lastFailureStatus: number | undefined;

/**
 * The sequence each document's local copy has in the Durable Object, or undefined when any local
 * copy is not the one it holds (a write not yet mirrored, or a file changed behind the mirror).
 */
function mirroredSequences(): Record<DurableDocumentName, number> | undefined {
  const dir = getConfigDir();
  const seqs = {} as Record<DurableDocumentName, number>;
  for (const name of Object.keys(DURABLE_DOCUMENT_FILES) as DurableDocumentName[]) {
    const state = readSequenceState(join(dir, sequenceFileFor(name)));
    if (!state.mirrored) return undefined;
    if (state.digest !== undefined) {
      const file = join(dir, DURABLE_DOCUMENT_FILES[name]);
      if (!existsSync(file) || documentDigest(readFileSync(file, "utf8")) !== state.digest) return undefined;
    }
    seqs[name] = state.seq;
  }
  return seqs;
}

/**
 * How long this process would keep giving the same answer, or 0 when it cannot say. Discovered
 * lists last until the first provider this process would query again (a stale cache entry, or a
 * failure's cooldown ending); a Codex catalog also embeds the template the catalog files hold, which
 * catalog sync rewrites, so it keeps ocx's own cache time. Anything else is fixed by the documents.
 */
function answerLifetimeMs(url: URL, config: OcxConfig, now: number): number {
  const ttl = config.modelCacheTtlMs ?? DEFAULT_MODEL_CACHE_TTL_MS;
  let until = url.searchParams.has("client_version") ? now + ttl : Number.POSITIVE_INFINITY;
  for (const [name, provider] of Object.entries(config.providers)) {
    if (provider.disabled === true || !providerSupportsLiveModelDiscovery(name, provider)) continue;
    until = Math.min(until, providerModelsFreshUntil(name, ttl, now));
  }
  return until === Number.POSITIVE_INFINITY ? Number.MAX_SAFE_INTEGER : Math.max(0, until - now);
}

async function publish(url: URL, headers: Headers, response: Response, config: OcxConfig, before: string): Promise<void> {
  const key = await modelListReplayKey(url, headers);
  if (!key) return;
  const ttlMs = answerLifetimeMs(url, config, Date.now());
  if (ttlMs <= 0) return;
  const body = await response.text();
  // Tagged with sequences that held from before the answer was built until after, so a write in
  // between can never label an older answer with newer state.
  const seqs = mirroredSequences();
  if (!seqs || JSON.stringify(seqs) !== before) return;
  const put = await stateRequest(`/model-lists/${key}`, {
    method: "PUT",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ body, headers: [...response.headers].filter(([name]) => REPLAYED_HEADERS.has(name)), seqs, ttlMs }),
  });
  await put?.body?.cancel();
  if (put && !put.ok && put.status !== lastFailureStatus) {
    lastFailureStatus = put.status;
    console.warn(`[models] answer not stored for the Worker (HTTP ${put.status}); requests keep reaching ocx`);
  }
}

/**
 * Returns what the route answers, publishing a copy when the answer is one the Worker could have
 * given: the Worker admits only the environment data token, so answers for other keys (whose model
 * scope may differ) are never published, and answers with native ChatGPT rows depend on Codex
 * entitlements this process resolves from the network.
 */
export function modelListReplay(req: Request, url: URL, admission: DataPlaneAdmission, config: OcxConfig): (response: Response) => Response {
  if (process.env.OCX_WORKER_NATIVE_STATE !== "1" || !durableMirrorEnabled() || admission.kind !== "environment") return response => response;
  if (shouldIncludeNativeOpenAi(config) || shouldIncludeAccountBoundNativeOpenAi(config)) return response => response;
  if (modelListReplayInputs(url, req.headers) === undefined) return response => response;
  const before = JSON.stringify(mirroredSequences() ?? null);
  return response => {
    if (response.status !== 200) return response;
    publish(url, req.headers, response.clone(), config, before).catch(error =>
      console.warn(`[models] answer not published for the Worker: ${error instanceof Error ? error.message : String(error)}`));
    return response;
  };
}

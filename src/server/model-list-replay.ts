// On a Cloudflare deployment, hands each GET /v1/models answer to the Durable Object so the Worker
// can replay it without starting the container (deploy/cloudflare/src/index.ts). A replay is served
// only while ocx's own model cache would still hold what the answer was built from, and while auth,
// Codex accounts and config are unchanged; see LeaseState.modelListRead.
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { getConfigDir } from "../config/paths";
import { DEFAULT_MODEL_CACHE_TTL_MS } from "../codex/model-cache";
import {
  DURABLE_DOCUMENT_FILES, documentDigest, durableMirrorEnabled, readSequenceState, sequenceFileFor, stateRequest,
  type DurableDocumentName,
} from "../lib/durable-mirror";
import type { OcxConfig } from "../types";
import type { DataPlaneAdmission } from "./auth-cors";
import { modelListReplayKey } from "./model-list-replay-key";

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

async function publish(url: URL, headers: Headers, response: Response, ttlMs: number, before: string): Promise<void> {
  const key = await modelListReplayKey(url, headers);
  if (!key) return;
  const body = await response.text();
  // Tagged with sequences that held from before the answer was built until after, so a write in
  // between can never label an older answer with newer state.
  const seqs = mirroredSequences();
  if (!seqs || JSON.stringify(seqs) !== before) return;
  await stateRequest(`/model-lists/${key}`, {
    method: "PUT",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ body, headers: [...response.headers], seqs, ttlMs }),
  });
}

/**
 * Returns what the route answers, publishing a copy when the answer is one the Worker could have
 * given: the Worker admits only the environment data token, so answers for other keys (whose model
 * scope may differ) are never published.
 */
export function modelListReplay(req: Request, url: URL, admission: DataPlaneAdmission, config: OcxConfig): (response: Response) => Response {
  if (!durableMirrorEnabled() || admission.kind !== "environment") return response => response;
  const before = JSON.stringify(mirroredSequences() ?? null);
  return response => {
    if (response.status !== 200) return response;
    publish(url, req.headers, response.clone(), config.modelCacheTtlMs ?? DEFAULT_MODEL_CACHE_TTL_MS, before).catch(error =>
      console.warn(`[models] answer not published for the Worker: ${error instanceof Error ? error.message : String(error)}`));
    return response;
  };
}

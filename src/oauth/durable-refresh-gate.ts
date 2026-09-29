/**
 * The container's side of the Durable Object's OAuth refresh arbitration
 * (deploy/cloudflare/src/oauth-refresh.ts). A Worker can now spend a rotating refresh token
 * through the hub's lease; this gate runs inside the container's own refresh-intent lock, after
 * the local intent file is written and before the provider call, so the hub sees every spend.
 *
 * Ordering: leaseCheck is the read of a single-writer row. A live lease means the Worker is
 * spending this exact stored generation, so the container waits for the hub's auth.json copy to
 * move and adopts it. No lease (or a hub that predates or cannot be reached) means the container
 * refreshes exactly as it did before — that path is still correct on its own.
 *
 * Adoption writes the DO's body bytes verbatim rather than re-serializing: the DO copy is the
 * document a later boot restores, so the local file must hold the same bytes for the same seq.
 */
import { join } from "node:path";
import { getConfigDir, atomicWriteFile, hardenConfigDir } from "../config";
import {
  documentDigest,
  DOCUMENT_SEQUENCE_HEADER,
  durableMirrorEnabled,
  readSequenceState,
  sequenceFileFor,
  stateRequest,
  writeSequenceState,
} from "../lib/durable-mirror";
import { createOAuthFileLock, credentialGeneration, getAuthStoreLockPath, getAuthStorePath } from "./store";
import type { OAuthCredentials } from "./types";

/** What a lease probe answered, mapped onto what the caller may do next. */
export type DurableOAuthRefreshGate =
  | { kind: "local" }
  | { kind: "adopted"; credential: OAuthCredentials }
  | { kind: "blocked" };

const LEASE_CHECK_PATH = "/oauth-refresh/lease-check";
// A Worker's refresh is one provider round trip: three seconds of polling covers it while keeping
// the caller's request inside its own retry budget. A lease that outlives the wait stays fenced
// by its TTL on the DO side.
const ADOPT_POLL_MS = 250;
const ADOPT_DEADLINE_MS = 3_000;

type Rec = Record<string, unknown>;
const isRec = (value: unknown): value is Rec => !!value && typeof value === "object" && !Array.isArray(value);

/** The credential the DO's auth.json copy carries for this account, or undefined. */
function accountCredentialIn(documentText: string, provider: string, accountId: string): OAuthCredentials | undefined {
  let parsed: unknown;
  try { parsed = JSON.parse(documentText); } catch { return undefined; }
  const set = isRec(parsed) ? parsed[provider] : undefined;
  const accounts = isRec(set) && Array.isArray(set.accounts) ? set.accounts as unknown[] : undefined;
  const account = accounts?.find(candidate => isRec(candidate) && candidate.id === accountId);
  const credential = isRec(account) ? account.credential : undefined;
  if (!isRec(credential)
    || typeof credential.access !== "string"
    || typeof credential.refresh !== "string"
    || typeof credential.expires !== "number") return undefined;
  return credential as OAuthCredentials;
}

/**
 * Writes the DO's document into the local auth.json, fenced by the same file lock every store
 * mutation takes so the bytes can never interleave with a local write. The sequence file records
 * the DO's own seq as already mirrored: the next local commit continues numbering above it and the
 * write-through retry never replays a body the DO already has.
 */
async function adoptDurableAuthDocument(body: string, seq: number): Promise<void> {
  const guard = await createOAuthFileLock({ path: getAuthStoreLockPath(), staleAfterMs: 30_000 }).acquire();
  try {
    hardenConfigDir();
    atomicWriteFile(getAuthStorePath(), body);
    writeSequenceState(join(getConfigDir(), sequenceFileFor("auth")), {
      seq,
      mirrored: true,
      digest: documentDigest(body),
    });
  } finally {
    guard.release();
  }
}

/** Reads the hub's auth.json copy; null when the DO has none or the request cannot be made. */
async function durableAuthDocument(): Promise<{ body: string; seq: number } | null> {
  const request = stateRequest("/documents/auth");
  if (!request) return null;
  const response = await request.catch(() => null);
  if (!response?.ok) return null;
  const seq = Number(response.headers.get(DOCUMENT_SEQUENCE_HEADER));
  const body = await response.text().catch(() => "");
  return Number.isSafeInteger(seq) && body ? { body, seq } : null;
}

/**
 * Runs before the container spends a refresh token on a deployment with a state mirror. `local`
 * means refresh as usual; `adopted` means the hub's copy already holds a newer generation, now
 * written through to auth.json; `blocked` means a Worker lease was seen and the outcome stayed
 * ambiguous, so this attempt must not spend (a retry later decides again).
 */
export async function gateDurableOAuthRefresh(
  provider: string,
  accountId: string,
  expectedGeneration: string,
  sleep: (ms: number) => Promise<void> = ms => Bun.sleep(ms),
  now: () => number = Date.now,
): Promise<DurableOAuthRefreshGate> {
  if (!durableMirrorEnabled()) return { kind: "local" };
  // The DO's document is the first thing to check, before the lease: a committed rotation frees
  // the lease row, so a store that already moved past the credential this process holds is the
  // common post-commit window — adopt it rather than spend the spent refresh token again. A
  // document whose seq the local file already recorded is older than us, never newer.
  const localSeq = () => readSequenceState(join(getConfigDir(), sequenceFileFor("auth"))).seq;
  const adoptable = (update: { body: string; seq: number }, credential: OAuthCredentials) =>
    update.seq > localSeq() && credentialGeneration(credential) !== expectedGeneration;
  const document = await durableAuthDocument();
  if (document) {
    const credential = accountCredentialIn(document.body, provider, accountId);
    if (credential && adoptable(document, credential)) {
      await adoptDurableAuthDocument(document.body, document.seq);
      return { kind: "adopted", credential };
    }
  }
  const leaseRequest = stateRequest(LEASE_CHECK_PATH, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ provider, accountId }),
  });
  if (!leaseRequest) return { kind: "local" };
  const probe = await leaseRequest.catch(() => null);
  // The hub is unreachable or predates the arbiter: the container is the only refresher, so its
  // local intent path applies unchanged.
  if (!probe?.ok) return { kind: "local" };
  let answer: unknown;
  try { answer = await probe.json(); } catch { return { kind: "local" }; }
  if (!(isRec(answer) && answer.live === true)) return { kind: "local" };

  // A Worker holds the lease for this stored generation. Its commit lands as a newer auth.json
  // document, so poll for that instead of spending the token ourselves.
  const deadline = now() + ADOPT_DEADLINE_MS;
  while (now() < deadline) {
    await sleep(ADOPT_POLL_MS);
    const update = await durableAuthDocument();
    if (!update) break;
    const credential = accountCredentialIn(update.body, provider, accountId);
    if (!credential) return { kind: "blocked" };
    if (adoptable(update, credential)) {
      await adoptDurableAuthDocument(update.body, update.seq);
      return { kind: "adopted", credential };
    }
  }
  // The lease vanished or the deadline passed without the commit arriving. Spending now could be
  // a second spend of a token the Worker already rotated, so this attempt declines to refresh;
  // the next resolve re-probes a lease that is either gone or committed by then.
  const recheckRequest = stateRequest(LEASE_CHECK_PATH, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ provider, accountId }),
  });
  const recheck = recheckRequest ? await recheckRequest.catch(() => null) : null;
  if (!recheck?.ok) return { kind: "blocked" };
  try {
    const again = await recheck.json() as unknown;
    if (isRec(again) && again.live === true) return { kind: "blocked" };
  } catch { return { kind: "blocked" }; }
  // The lease is gone and no commit landed: the Worker died before dispatching or the provider
  // definitively rejected it. The remaining window is that it died after dispatch, which the
  // local intent file treats as uncertain too — block this attempt rather than risk the spend.
  return { kind: "blocked" };
}

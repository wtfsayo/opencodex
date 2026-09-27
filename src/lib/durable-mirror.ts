// On a Cloudflare Container deployment the disk is wiped whenever the instance sleeps or rolls out,
// and the supervisor's periodic snapshot lags by up to one interval. A login or a rotated refresh
// token saved in that window came back as the old one, which for a rotating refresh token means a
// forced re-login. The supervisor (docker/cloudflare-supervisor.ts) sets the boot id below; with it,
// every commit to a credential store is also written to the hub's Durable Object, and the next boot
// restores whichever of that copy and the snapshot's file is newer. Without it, nothing here runs.
//
// Newer is decided by a sequence number kept next to each file. The Durable Object refuses a write
// that is not newer than what it holds, so a slow retry can never replace a later commit.
//
// Dependency-free on purpose: the supervisor imports it before ocx starts.
import { createHash } from "node:crypto";
import { readFileSync, renameSync, writeFileSync } from "node:fs";

export const DURABLE_STATE_BOOT_ID_ENV = "OCX_STATE_BOOT_ID";
export const DOCUMENT_SEQUENCE_HEADER = "x-ocx-document-seq";
/** Each mirrored document and the file it is restored to, relative to the opencodex home. */
export const DURABLE_DOCUMENT_FILES = {
  auth: "auth.json",
  "codex-accounts": "codex-accounts.json",
} as const;
export type DurableDocumentName = keyof typeof DURABLE_DOCUMENT_FILES;
/** Sits next to the document's file: the sequence of its content, and whether the Durable Object has it. */
export const sequenceFileFor = (name: DurableDocumentName): string => `${DURABLE_DOCUMENT_FILES[name]}.seq`;
// Intercepted by OpencodexHub.outboundByHost in deploy/cloudflare/src/index.ts; never reaches DNS.
const STATE_ORIGIN = "http://state.ocx.internal";
const BOOT_ID_PATTERN = /^[0-9a-f]{32}$/;
// Kept well under the auth store's 30 s mutation-queue wait and file-lock staleness.
const ATTEMPTS = 2;
const ATTEMPT_TIMEOUT_MS = 5_000;
const CATCH_UP_MIN_MS = 5_000;
const CATCH_UP_MAX_MS = 60_000;

export class DurableMirrorError extends Error {
  readonly code = "DURABLE_STATE_LEASE_LOST";
  constructor(message: string) {
    super(message);
    this.name = "DurableMirrorError";
  }
}

/** `digest` is of the content `seq` names; a file that no longer matches it was changed unrecorded. */
export type SequenceState = { seq: number; mirrored: boolean; digest?: string };

export function documentDigest(body: string): string {
  return createHash("sha256").update(body).digest("hex");
}

export function readSequenceState(path: string): SequenceState {
  try {
    const value = JSON.parse(readFileSync(path, "utf8")) as { seq?: unknown; mirrored?: unknown; digest?: unknown };
    if (typeof value.seq === "number" && Number.isSafeInteger(value.seq) && value.seq >= 0) {
      return { seq: value.seq, mirrored: value.mirrored !== false, ...(typeof value.digest === "string" ? { digest: value.digest } : {}) };
    }
  } catch { /* absent or unreadable: nothing was ever mirrored from this home */ }
  return { seq: 0, mirrored: true };
}

export function writeSequenceState(path: string, state: SequenceState): void {
  const temporary = `${path}.${process.pid}.tmp`;
  writeFileSync(temporary, `${JSON.stringify(state)}\n`, { mode: 0o600 });
  renameSync(temporary, path);
}

/** Never lets bookkeeping stop a credential write: the provider may already have rotated the token. */
function recordSequence(path: string, state: SequenceState): void {
  try {
    writeSequenceState(path, state);
  } catch (error) {
    console.warn(`[state] Could not record the document sequence: ${error instanceof Error ? error.message : String(error)}`);
  }
}

type MirrorTransport = {
  origin: string;
  fetch: (url: string, init: RequestInit) => Promise<Response>;
  sleep: (ms: number) => Promise<void>;
  schedule: (run: () => void, ms: number) => { cancel(): void };
};
const defaultTransport: MirrorTransport = {
  origin: STATE_ORIGIN,
  fetch: (url, init) => fetch(url, init),
  sleep: ms => Bun.sleep(ms),
  schedule: (run, ms) => {
    const timer = setTimeout(run, ms);
    timer.unref?.();
    return { cancel: () => clearTimeout(timer) };
  },
};
let transport = defaultTransport;

type DocumentState = { lastSeq: number; catchUp?: { cancel(): void } };
const documents = new Map<DurableDocumentName, DocumentState>();
function stateOf(name: DurableDocumentName): DocumentState {
  let state = documents.get(name);
  if (!state) documents.set(name, state = { lastSeq: 0 });
  return state;
}

export function setDurableMirrorTransportForTests(next: Partial<MirrorTransport> | null): void {
  transport = next ? { ...defaultTransport, ...next } : defaultTransport;
  for (const state of documents.values()) state.catchUp?.cancel();
  documents.clear();
}

export function durableMirrorEnabled(): boolean {
  const value = process.env[DURABLE_STATE_BOOT_ID_ENV];
  return !!value && BOOT_ID_PATTERN.test(value);
}

/** Takes the next sequence for a new local write and drops any retry of an older one. */
function nextSequence(name: DurableDocumentName, statePath: string): number {
  const state = stateOf(name);
  state.catchUp?.cancel();
  state.catchUp = undefined;
  state.lastSeq = Math.max(state.lastSeq, readSequenceState(statePath).seq) + 1;
  return state.lastSeq;
}

type PutResult = { durable: true; seq: number } | { durable: false; seq: number; retry: boolean };

async function put(name: DurableDocumentName, body: string, seq: number): Promise<PutResult> {
  const bootId = process.env[DURABLE_STATE_BOOT_ID_ENV]!;
  const state = stateOf(name);
  for (let attempt = 1; attempt <= ATTEMPTS; attempt++) {
    let response: Response | undefined;
    try {
      response = await transport.fetch(`${transport.origin}/documents/${name}`, {
        method: "PUT",
        body,
        headers: { "content-type": "application/json", "x-ocx-boot-id": bootId, [DOCUMENT_SEQUENCE_HEADER]: String(seq) },
        signal: AbortSignal.timeout(ATTEMPT_TIMEOUT_MS),
      });
    } catch { /* network error or timeout: the attempt may still land, which the sequence makes harmless */ }
    if (response?.ok) return { durable: true, seq };
    if (response?.status === 409) throw new DurableMirrorError("the state lease moved to another container; not saving credentials");
    if (response?.status === 412) {
      const stored = Number(response.headers.get(DOCUMENT_SEQUENCE_HEADER));
      if (!Number.isSafeInteger(stored)) return { durable: false, seq, retry: false };
      // An earlier attempt of this same write already landed.
      if (stored === seq) return { durable: true, seq };
      // A newer local write superseded this one and mirrors itself.
      if (state.lastSeq !== seq) return { durable: false, seq, retry: false };
      // This is the newest local write and this process holds the lease, so a stored sequence ahead
      // of it means only that the local sequence file was behind. That rests on one process per
      // container writing these stores: the supervisor's ocx, the only one given the boot id.
      if (stored > seq) {
        seq = state.lastSeq = stored + 1;
        continue;
      }
    }
    if (response && response.status >= 400 && response.status < 500 && response.status !== 412) {
      console.warn(`[state] Durable Object refused ${name} (${response.status}); it stays in the local file and the snapshot.`);
      return { durable: false, seq, retry: false };
    }
    if (attempt < ATTEMPTS) await transport.sleep(500);
  }
  return { durable: false, seq, retry: true };
}

/** Mirrors `body` at `seq` until it lands, a newer write supersedes it, or the lease is lost. */
function mirrorUntilDurable(name: DurableDocumentName, body: string, seq: number, statePath: string, delayMs: number): void {
  void put(name, body, seq).then(result => {
    const state = stateOf(name);
    if (state.lastSeq !== result.seq) return;
    if (result.durable) {
      recordSequence(statePath, { seq: result.seq, mirrored: true, digest: documentDigest(body) });
    } else if (result.retry) {
      state.catchUp = transport.schedule(() => {
        state.catchUp = undefined;
        mirrorUntilDurable(name, body, result.seq, statePath, Math.min(delayMs * 2, CATCH_UP_MAX_MS));
      }, delayMs);
    }
  }).catch(error => console.error(`[state] Mirroring ${name} stopped: ${error instanceof Error ? error.message : String(error)}`));
}

export type MirrorCommit = {
  readonly durable: boolean;
  /** Call after the local file is written: records the sequence, or keeps retrying a failed mirror. */
  settle(): void;
};

/**
 * For writers that can await: mirrors the whole document ahead of the local file, or returns null
 * when the deployment has none. A failure does not stop the local write: a refresh token the
 * provider has already rotated must reach the disk, and a rejected commit here would discard it.
 * The sequence file is marked unmirrored first, so the next boot restores the local copy instead.
 * Throws only when the lease is gone, in which case the supervisor is already stopping this
 * container without saving anything.
 */
export async function mirrorBeforeWrite(name: DurableDocumentName, body: string, statePath: string): Promise<MirrorCommit | null> {
  if (!durableMirrorEnabled()) return null;
  const result = await put(name, body, nextSequence(name, statePath));
  if (!result.durable) {
    recordSequence(statePath, { seq: result.seq, mirrored: false, digest: documentDigest(body) });
    console.warn(`[state] Could not reach the Durable Object; ${name} was saved locally and will be retried.`);
  }
  return {
    durable: result.durable,
    settle() {
      if (result.durable) recordSequence(statePath, { seq: result.seq, mirrored: true, digest: documentDigest(body) });
      else if (result.retry) {
        const state = stateOf(name);
        state.catchUp = transport.schedule(() => {
          state.catchUp = undefined;
          mirrorUntilDurable(name, body, result.seq, statePath, CATCH_UP_MIN_MS * 2);
        }, CATCH_UP_MIN_MS);
      }
    },
  };
}

export type PendingLocalWrite = {
  /** Call once the local file holds the body; the mirror then runs in the background. */
  written(): void;
};

/**
 * For synchronous writers, which cannot wait for the network: call before writing the file. The
 * sequence file is marked unmirrored before the write, so a crash anywhere between here and the
 * Durable Object accepting the copy leaves the next boot preferring the local file.
 */
export function beginLocalWrite(name: DurableDocumentName, statePath: string, body: string): PendingLocalWrite | null {
  if (!durableMirrorEnabled()) return null;
  const seq = nextSequence(name, statePath);
  recordSequence(statePath, { seq, mirrored: false, digest: documentDigest(body) });
  return { written: () => mirrorUntilDurable(name, body, seq, statePath, CATCH_UP_MIN_MS) };
}

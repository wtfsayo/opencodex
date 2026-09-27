// On a Cloudflare Container deployment the disk is wiped whenever the instance sleeps or rolls out,
// and the supervisor's periodic snapshot lags by up to one interval. A login or a rotated refresh
// token saved in that window came back as the old one, which for a rotating refresh token means a
// forced re-login. The supervisor (docker/cloudflare-supervisor.ts) sets the boot id below; with it,
// every auth-store commit is also written to the hub's Durable Object, and the next boot restores
// whichever of that copy and the snapshot's file is newer. Without it, nothing here runs.
//
// Newer is decided by a sequence number kept next to auth.json. The Durable Object refuses a write
// that is not newer than what it holds, so a slow retry can never replace a later commit.
//
// Dependency-free on purpose: the supervisor imports it before ocx starts.
import { readFileSync, renameSync, writeFileSync } from "node:fs";

export const DURABLE_STATE_BOOT_ID_ENV = "OCX_STATE_BOOT_ID";
export const DOCUMENT_SEQUENCE_HEADER = "x-ocx-document-seq";
/** Sits next to auth.json: the sequence of its content, and whether the Durable Object has it. */
export const AUTH_STORE_SEQUENCE_FILE = "auth.json.seq";
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

export type SequenceState = { seq: number; mirrored: boolean };

export function readSequenceState(path: string): SequenceState {
  try {
    const value = JSON.parse(readFileSync(path, "utf8")) as { seq?: unknown; mirrored?: unknown };
    if (typeof value.seq === "number" && Number.isSafeInteger(value.seq) && value.seq >= 0) {
      return { seq: value.seq, mirrored: value.mirrored !== false };
    }
  } catch { /* absent or unreadable: nothing was ever mirrored from this home */ }
  return { seq: 0, mirrored: true };
}

export function writeSequenceState(path: string, state: SequenceState): void {
  const temporary = `${path}.${process.pid}.tmp`;
  writeFileSync(temporary, `${JSON.stringify(state)}\n`, { mode: 0o600 });
  renameSync(temporary, path);
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
let lastSeq = 0;
let catchUp: { cancel(): void } | undefined;

export function setDurableMirrorTransportForTests(next: Partial<MirrorTransport> | null): void {
  transport = next ? { ...defaultTransport, ...next } : defaultTransport;
  catchUp?.cancel();
  catchUp = undefined;
  lastSeq = 0;
}

export function durableMirrorEnabled(): boolean {
  const value = process.env[DURABLE_STATE_BOOT_ID_ENV];
  return !!value && BOOT_ID_PATTERN.test(value);
}

type PutResult = { durable: true; seq: number } | { durable: false; seq: number; retry: boolean };

async function put(body: string, seq: number, mayAdvance: boolean): Promise<PutResult> {
  const bootId = process.env[DURABLE_STATE_BOOT_ID_ENV]!;
  for (let attempt = 1; attempt <= ATTEMPTS; attempt++) {
    let response: Response | undefined;
    try {
      response = await transport.fetch(`${transport.origin}/documents/auth`, {
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
      // An earlier attempt of this same write already landed.
      if (stored === seq) return { durable: true, seq };
      // A commit holds the newest store, so a stored sequence ahead of it means only that the
      // local sequence file was behind. A catch-up retry must not do this: what is ahead of it is
      // the newer commit that superseded it.
      if (!mayAdvance) return { durable: false, seq, retry: false };
      if (Number.isSafeInteger(stored) && stored > seq) {
        seq = stored + 1;
        continue;
      }
    }
    if (response && response.status >= 400 && response.status < 500 && response.status !== 412) {
      console.warn(`[oauth] Durable Object refused the auth store (${response.status}); it stays in the local file and the snapshot.`);
      return { durable: false, seq, retry: false };
    }
    if (attempt < ATTEMPTS) await transport.sleep(500);
  }
  return { durable: false, seq, retry: true };
}

function scheduleCatchUp(body: string, seq: number, statePath: string, delayMs: number): void {
  catchUp = transport.schedule(() => {
    catchUp = undefined;
    void put(body, seq, false).then(result => {
      if (lastSeq !== seq) return; // a newer commit superseded this one and mirrors itself
      if (result.durable) {
        lastSeq = result.seq;
        writeSequenceState(statePath, { seq: result.seq, mirrored: true });
      } else if (result.retry) {
        scheduleCatchUp(body, seq, statePath, Math.min(delayMs * 2, CATCH_UP_MAX_MS));
      }
    }).catch(error => console.error(`[oauth] Auth store catch-up stopped: ${error instanceof Error ? error.message : String(error)}`));
  }, delayMs);
}

export type MirrorCommit = {
  readonly durable: boolean;
  /** Call after the local file is written: records the sequence and keeps retrying a failed mirror. */
  settle(): void;
};

/**
 * Writes the whole auth store to the Durable Object ahead of the local file, or returns null when
 * the deployment has none. A failure does not stop the local write: a refresh token the provider has
 * already rotated must reach the disk, and a rejected commit here would discard it. The next boot
 * restores the local copy instead, because its sequence is newer. Throws only when the lease is gone,
 * in which case the supervisor is already stopping this container without saving anything.
 */
export async function mirrorAuthStore(body: string, statePath: string): Promise<MirrorCommit | null> {
  if (!durableMirrorEnabled()) return null;
  catchUp?.cancel();
  catchUp = undefined;
  const result = await put(body, Math.max(lastSeq, readSequenceState(statePath).seq) + 1, true);
  lastSeq = result.seq;
  if (!result.durable) console.warn("[oauth] Could not reach the Durable Object; the auth store was saved locally and will be retried.");
  return {
    durable: result.durable,
    settle() {
      writeSequenceState(statePath, { seq: result.seq, mirrored: result.durable });
      if (!result.durable && result.retry) scheduleCatchUp(body, result.seq, statePath, CATCH_UP_MIN_MS);
    },
  };
}

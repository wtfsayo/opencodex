// On a Cloudflare Container deployment the disk is wiped whenever the instance sleeps or rolls out,
// and the supervisor's periodic snapshot lags by up to one interval. A login or a rotated refresh
// token saved in that window came back as the old one, which for a rotating refresh token means a
// forced re-login. The supervisor (docker/cloudflare-supervisor.ts) sets the boot id below; with it,
// every auth-store commit reaches the hub's Durable Object before the local file, and the next boot
// restores that copy over the snapshot. Without it, nothing here runs.
//
// Dependency-free on purpose: the supervisor imports the env name before ocx starts.

export const DURABLE_STATE_BOOT_ID_ENV = "OCX_STATE_BOOT_ID";
// Intercepted by OpencodexHub.outboundByHost in deploy/cloudflare/src/index.ts; never reaches DNS.
const STATE_ORIGIN = "http://state.ocx.internal";
const BOOT_ID_PATTERN = /^[0-9a-f]{32}$/;
const ATTEMPTS = 3;
const ATTEMPT_TIMEOUT_MS = 10_000;

export class DurableMirrorError extends Error {
  readonly code = "DURABLE_STATE_UNAVAILABLE";
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = "DurableMirrorError";
  }
}

type MirrorTransport = {
  origin: string;
  fetch: (url: string, init: RequestInit) => Promise<Response>;
  sleep: (ms: number) => Promise<void>;
};
const defaultTransport: MirrorTransport = { origin: STATE_ORIGIN, fetch: (url, init) => fetch(url, init), sleep: ms => Bun.sleep(ms) };
let transport = defaultTransport;

export function setDurableMirrorTransportForTests(next: Partial<MirrorTransport> | null): void {
  transport = next ? { ...defaultTransport, ...next } : defaultTransport;
}

function bootId(): string | null {
  const value = process.env[DURABLE_STATE_BOOT_ID_ENV];
  return value && BOOT_ID_PATTERN.test(value) ? value : null;
}

/**
 * Writes the whole auth store to the Durable Object, or throws. Callers commit it before the local
 * file so the durable copy is never older than the one on disk; a throw means nothing was persisted,
 * exactly as a failed disk write. A 409 means another container holds the state lease, so this one
 * is being fenced and must not retry.
 */
export async function mirrorAuthStore(body: string): Promise<void> {
  const id = bootId();
  if (!id) return;
  let lastError: unknown;
  for (let attempt = 1; attempt <= ATTEMPTS; attempt++) {
    let response: Response;
    try {
      response = await transport.fetch(`${transport.origin}/documents/auth`, {
        method: "PUT",
        body,
        headers: { "content-type": "application/json", "x-ocx-boot-id": id },
        signal: AbortSignal.timeout(ATTEMPT_TIMEOUT_MS),
      });
    } catch (error) {
      response = Response.error();
      lastError = error;
    }
    if (response.ok) return;
    if (response.status === 409) throw new DurableMirrorError("the state lease moved to another container; not saving credentials");
    if (response.status >= 400 && response.status < 500) throw new DurableMirrorError(`durable auth store refused the write: ${response.status}`);
    if (response.status >= 500) lastError = new Error(`status ${response.status}`);
    if (attempt < ATTEMPTS) await transport.sleep(250 * 2 ** attempt);
  }
  throw new DurableMirrorError("durable auth store is unreachable; credentials were not saved", { cause: lastError });
}

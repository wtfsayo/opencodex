// Only one container may write the snapshot. A rollout stops the old instance before starting the
// new one, but a container that dies without SIGTERM never releases, so the next waits for staleness.
export const LEASE_STALE_MS = 120_000;

export type Lease = { bootId: string; heartbeatAt: number };

export type LeaseDecision =
  | { granted: true; lease: Lease }
  | { granted: false; retryAfterSeconds: number };

export function decideLease(current: Lease | undefined, bootId: string, now: number): LeaseDecision {
  if (!current || current.bootId === bootId || now - current.heartbeatAt >= LEASE_STALE_MS) {
    return { granted: true, lease: { bootId, heartbeatAt: now } };
  }
  const remaining = LEASE_STALE_MS - (now - current.heartbeatAt);
  return { granted: false, retryAfterSeconds: Math.max(1, Math.ceil(remaining / 1000)) };
}

export function isHolder(current: Lease | undefined, bootId: string): boolean {
  return current?.bootId === bootId;
}

export const BOOT_ID_PATTERN = /^[0-9a-f]{32}$/;

export interface LeaseStorage {
  get<T>(key: string): Promise<T | undefined>;
  put<T>(key: string, value: T): Promise<void>;
  delete(key: string): Promise<boolean>;
  /** Keys under `prefix` in key order, at most `limit`. */
  list<T>(options: { prefix: string; limit: number }): Promise<Map<string, T>>;
}

const USAGE_KEY_PREFIX = "ocx:usage:";
const USAGE_SEQ_KEY = "ocx:usage-seq";
// Rows the Worker served while ocx was not running to take them. Past this the oldest are dropped:
// a hub whose container never starts must not grow its storage without end.
export const MAX_QUEUED_USAGE = 20_000;
const usageKey = (seq: number) => `${USAGE_KEY_PREFIX}${String(seq).padStart(12, "0")}`;

const LEASE_KEY = "ocx:lease";
const SNAPSHOT_KEY = "ocx:snapshot";
const DOCUMENT_KEY_PREFIX = "ocx:document:";

/**
 * Stores written through to the Durable Object on every commit instead of waiting for the next
 * snapshot. Each is one whole JSON document, restored over the snapshot's copy at boot.
 */
export const DURABLE_DOCUMENTS = ["auth", "codex-accounts", "config"] as const;
export type DurableDocument = (typeof DURABLE_DOCUMENTS)[number];
// SQLite-backed Durable Objects cap a stored value at 2 MiB; these stores are a few KiB.
export const MAX_DOCUMENT_BYTES = 1024 * 1024;
export type StoredDocument = { body: string; seq: number };
export type DocumentCommit = { kind: "committed" } | { kind: "lease-lost" } | { kind: "stale"; storedSeq: number };

/** The Durable Object's state. Awaiting DO storage keeps the input gate closed, so these read-modify-writes need no CAS. */
export class LeaseState {
  constructor(private readonly storage: LeaseStorage, private readonly now: () => number = Date.now) {}

  async acquireLease(bootId: string): Promise<{ granted: boolean; retryAfterSeconds?: number }> {
    const decision = decideLease(await this.storage.get<Lease>(LEASE_KEY), bootId, this.now());
    if (!decision.granted) return { granted: false, retryAfterSeconds: decision.retryAfterSeconds };
    await this.storage.put(LEASE_KEY, decision.lease);
    return { granted: true };
  }

  /** Extends only a lease the caller still holds, so a late heartbeat can never re-take a released or reassigned lease. */
  async renewLease(bootId: string): Promise<boolean> {
    if (!(await this.holdsLease(bootId))) return false;
    await this.storage.put(LEASE_KEY, { bootId, heartbeatAt: this.now() });
    return true;
  }

  async holdsLease(bootId: string): Promise<boolean> {
    return isHolder(await this.storage.get<Lease>(LEASE_KEY), bootId);
  }

  async releaseLease(bootId: string): Promise<void> {
    if (await this.holdsLease(bootId)) await this.storage.delete(LEASE_KEY);
  }

  currentSnapshot(): Promise<string | undefined> {
    return this.storage.get<string>(SNAPSHOT_KEY);
  }

  /**
   * Forgets the saved state so the next boot starts from the bootstrap config. Also drops the lease:
   * a reset runs only after the container has stopped, so any holder is a dead one.
   */
  async discardSnapshot(): Promise<string | undefined> {
    const discarded = await this.storage.get<string>(SNAPSHOT_KEY);
    await this.storage.delete(SNAPSHOT_KEY);
    for (const name of DURABLE_DOCUMENTS) await this.storage.delete(DOCUMENT_KEY_PREFIX + name);
    for (;;) {
      const queued = await this.storage.list({ prefix: USAGE_KEY_PREFIX, limit: 1000 });
      if (queued.size === 0) break;
      for (const key of queued.keys()) await this.storage.delete(key);
    }
    await this.storage.delete(LEASE_KEY);
    return discarded;
  }

  /** Returns the key the new snapshot replaced, or null when the caller lost the lease. */
  async commitSnapshot(bootId: string, key: string): Promise<{ replaced: string | undefined } | null> {
    if (!(await this.holdsLease(bootId))) return null;
    const replaced = await this.storage.get<string>(SNAPSHOT_KEY);
    await this.storage.put(SNAPSHOT_KEY, key);
    return { replaced: replaced === key ? undefined : replaced };
  }

  async readDocument(name: DurableDocument): Promise<StoredDocument | undefined> {
    const stored = await this.storage.get<Partial<StoredDocument>>(DOCUMENT_KEY_PREFIX + name);
    // Anything else was not written by commitDocument; treating it as absent lets the next commit replace it.
    return typeof stored?.body === "string" && Number.isSafeInteger(stored.seq) ? stored as StoredDocument : undefined;
  }

  /**
   * Stores `body` only if `seq` is newer than what is held: a retry that lands late must not replace
   * a later commit. A fenced container must not overwrite the holder's credentials at all.
   */
  async commitDocument(bootId: string, name: DurableDocument, body: string, seq: number): Promise<DocumentCommit> {
    if (!(await this.holdsLease(bootId))) return { kind: "lease-lost" };
    const current = await this.readDocument(name);
    if (current && current.seq >= seq) return { kind: "stale", storedSeq: current.seq };
    await this.storage.put<StoredDocument>(DOCUMENT_KEY_PREFIX + name, { body, seq });
    return { kind: "committed" };
  }

  /** Queues a usage row from a Worker-served turn for ocx to append to its usage log. */
  async enqueueUsage(row: unknown): Promise<void> {
    const seq = ((await this.storage.get<number>(USAGE_SEQ_KEY)) ?? 0) + 1;
    await this.storage.put(USAGE_SEQ_KEY, seq);
    await this.storage.put(usageKey(seq), row);
    if (seq > MAX_QUEUED_USAGE) await this.storage.delete(usageKey(seq - MAX_QUEUED_USAGE));
  }

  /** The oldest queued rows, for the lease holder only. */
  async peekUsage(bootId: string, limit: number): Promise<{ seq: number; row: unknown }[] | null> {
    if (!(await this.holdsLease(bootId))) return null;
    const queued = await this.storage.list<unknown>({ prefix: USAGE_KEY_PREFIX, limit });
    return [...queued].map(([key, row]) => ({ seq: Number(key.slice(USAGE_KEY_PREFIX.length)), row }));
  }

  /** Forgets rows the lease holder has appended to its log. */
  async ackUsage(bootId: string, seqs: readonly number[]): Promise<boolean> {
    if (!(await this.holdsLease(bootId))) return false;
    for (const seq of seqs) if (Number.isSafeInteger(seq) && seq > 0) await this.storage.delete(usageKey(seq));
    return true;
  }
}

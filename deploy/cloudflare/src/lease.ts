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

// The Worker's copy of ocx's per-session skills-catalog snapshot (src/server/responses/skills-snapshot.ts):
// the same TTL, per-block limit and session bound, in storage because a Worker has no process to
// hold them. Metadata sits apart from the blocks so pruning reads only small rows.
const SKILLS_BLOCK_PREFIX = "ocx:skills:";
const SKILLS_META_PREFIX = "ocx:skills-meta:";
export const SKILLS_SNAPSHOT_TTL_MS = 4 * 60 * 60 * 1000;
export const MAX_SKILLS_BLOCK_BYTES = 512 * 1024;
export const MAX_SKILLS_SESSIONS = 1000;
type SkillsMeta = { lastAccessed: number };

// /v1/models answers ocx computed, replayed by the Worker while ocx's own model cache would still
// hold them and the three documents are unchanged. See modelListRead.
const MODEL_LIST_PREFIX = "ocx:model-list:";
const MODEL_LIST_META_PREFIX = "ocx:model-list-meta:";
export const MAX_MODEL_LISTS = 32;
// A Codex catalog runs to about 24 KB a model; SQLite-backed objects take values up to 2 MiB.
export const MAX_MODEL_LIST_BYTES = 2_000_000;
// However long ocx says an answer stays its answer, it is replayed for at most this long.
export const MAX_MODEL_LIST_TTL_MS = 60 * 60 * 1000;
export type DocumentSeqs = Record<DurableDocument, number>;
export type ModelList = { body: string; headers: [string, string][] };
/** `stamp` names the Worker version and container environment the list was answered under. */
type ModelListMeta = { expiresAt: number; seqs: DocumentSeqs; stamp: string };
const DOCUMENT_SEQ_KEY_PREFIX = "ocx:document-seq:";

// ocx's reasoning-effort caches (src/providers/reasoning-metadata.ts), as the running ocx last
// published them, so the Worker maps effort as it would. Each kind keeps the newest version.
const REASONING_METADATA_PREFIX = "ocx:reasoning-metadata:";
export const REASONING_METADATA_KINDS = ["snapshot", "support"] as const;
export type ReasoningMetadataKind = (typeof REASONING_METADATA_KINDS)[number];
type StoredReasoningMetadata = { bootId: string; version: number; body: string };

// The Claude Code fingerprint headers naming ocx's runtime (src/server/worker-native-state.ts),
// under the stamp of the Worker version and container environment they were published under. Only
// the newest is kept: a Worker version other than the hub's must not reuse an older container's.
const CLIENT_RUNTIME_KEY = "ocx:client-runtime";
// What a ChatGPT passthrough turn reads from ocx's process (src/server/worker-native-state.ts), under
// the stamp it was published under. It outlives the process's sleep, since the next boot restores
// the same state, and stops counting once another boot holds the lease, until that one publishes.
const NATIVE_OPENAI_FACTS_KEY = "ocx:native-openai-facts";
type StoredNativeOpenAiFacts = { bootId: string; stamp: string; version: number; facts: unknown };
type StoredClientRuntime = { stamp: string; headers: Record<string, string> };

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
    for (const name of DURABLE_DOCUMENTS) {
      await this.storage.delete(DOCUMENT_KEY_PREFIX + name);
      await this.storage.delete(DOCUMENT_SEQ_KEY_PREFIX + name);
    }
    for (;;) {
      const queued = await this.storage.list({ prefix: USAGE_KEY_PREFIX, limit: 1000 });
      if (queued.size === 0) break;
      for (const key of queued.keys()) await this.storage.delete(key);
    }
    // Blocks are deleted by the keys their small metadata rows name, never listed: 1,000 of them
    // at the size limit would not fit in a Durable Object's memory.
    for (;;) {
      const metas = await this.storage.list<SkillsMeta>({ prefix: SKILLS_META_PREFIX, limit: 1000 });
      if (metas.size === 0) break;
      for (const key of metas.keys()) await this.dropSkills(key.slice(SKILLS_META_PREFIX.length));
    }
    for (const kind of REASONING_METADATA_KINDS) await this.storage.delete(REASONING_METADATA_PREFIX + kind);
    await this.storage.delete(CLIENT_RUNTIME_KEY);
    await this.storage.delete(NATIVE_OPENAI_FACTS_KEY);
    for (;;) {
      const metas = await this.storage.list<ModelListMeta>({ prefix: MODEL_LIST_META_PREFIX, limit: 1000 });
      if (metas.size === 0) break;
      for (const key of metas.keys()) await this.dropModelList(key.slice(MODEL_LIST_META_PREFIX.length));
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
    await this.storage.put<number>(DOCUMENT_SEQ_KEY_PREFIX + name, seq);
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

  /** The catalog block this session is frozen to while it is live (its expiry slides), else undefined. */
  async skillsSnapshotRead(scope: string): Promise<string | undefined> {
    const now = this.now();
    const meta = await this.storage.get<SkillsMeta>(SKILLS_META_PREFIX + scope);
    if (!meta || now - meta.lastAccessed > SKILLS_SNAPSHOT_TTL_MS) return undefined;
    const block = await this.storage.get<string>(SKILLS_BLOCK_PREFIX + scope);
    if (block === undefined) return undefined;
    await this.storage.put<SkillsMeta>(SKILLS_META_PREFIX + scope, { lastAccessed: now });
    return block;
  }

  /**
   * Freezes the session to `block` unless a live one is already stored: the first catalog wins, as in
   * ocx. Called only once the turn has been sent, as ocx stores only after admission.
   */
  async skillsSnapshotCommit(scope: string, block: string): Promise<void> {
    if ((await this.skillsSnapshotRead(scope)) !== undefined) return;
    if (new TextEncoder().encode(block).byteLength > MAX_SKILLS_BLOCK_BYTES) return;
    const now = this.now();
    await this.pruneSkills(now);
    // Metadata first: reset and pruning find blocks only through it, so a block must never exist
    // without its row.
    await this.storage.put<SkillsMeta>(SKILLS_META_PREFIX + scope, { lastAccessed: now });
    await this.storage.put(SKILLS_BLOCK_PREFIX + scope, block);
  }

  /** Drops expired sessions, then the least recently used until there is room for one more. */
  private async pruneSkills(now: number): Promise<void> {
    const metas = await this.storage.list<SkillsMeta>({ prefix: SKILLS_META_PREFIX, limit: MAX_SKILLS_SESSIONS + 50 });
    const live: [string, number][] = [];
    for (const [key, meta] of metas) {
      const scope = key.slice(SKILLS_META_PREFIX.length);
      if (now - meta.lastAccessed > SKILLS_SNAPSHOT_TTL_MS) await this.dropSkills(scope);
      else live.push([scope, meta.lastAccessed]);
    }
    live.sort((a, b) => a[1] - b[1]);
    while (live.length >= MAX_SKILLS_SESSIONS) await this.dropSkills(live.shift()![0]);
  }

  /**
   * A /v1/models answer ocx computed for `key`, while it is younger than ocx's model cache TTL and
   * every document still has the sequence it was computed under; else undefined.
   */
  async modelListRead(key: string, stamp: string): Promise<ModelList | undefined> {
    const meta = await this.storage.get<ModelListMeta>(MODEL_LIST_META_PREFIX + key);
    if (!meta || this.now() >= meta.expiresAt || meta.stamp !== stamp) return undefined;
    for (const name of DURABLE_DOCUMENTS) {
      if ((await this.documentSeq(name)) !== meta.seqs[name]) return undefined;
    }
    return this.storage.get<ModelList>(MODEL_LIST_PREFIX + key);
  }

  /** A document's sequence without reading its body (a document from before seq rows reads its body once). */
  private async documentSeq(name: DurableDocument): Promise<number> {
    return (await this.storage.get<number>(DOCUMENT_SEQ_KEY_PREFIX + name)) ?? (await this.readDocument(name))?.seq ?? 0;
  }

  /** Stores an answer from the lease holder, dropping expired ones and then the soonest to expire. */
  async modelListCommit(bootId: string, key: string, list: ModelList, seqs: DocumentSeqs, ttlMs: number, stamp: string): Promise<boolean> {
    if (!(await this.holdsLease(bootId))) return false;
    const now = this.now();
    const metas = await this.storage.list<ModelListMeta>({ prefix: MODEL_LIST_META_PREFIX, limit: MAX_MODEL_LISTS + 50 });
    const live: [string, number][] = [];
    for (const [metaKey, meta] of metas) {
      const listKey = metaKey.slice(MODEL_LIST_META_PREFIX.length);
      if (now >= meta.expiresAt) await this.dropModelList(listKey);
      else if (listKey !== key) live.push([listKey, meta.expiresAt]);
    }
    live.sort((a, b) => a[1] - b[1]);
    while (live.length >= MAX_MODEL_LISTS) await this.dropModelList(live.shift()![0]);
    // Metadata first, as for skills: reset finds bodies only through it.
    await this.storage.put<ModelListMeta>(MODEL_LIST_META_PREFIX + key, { expiresAt: now + Math.min(ttlMs, MAX_MODEL_LIST_TTL_MS), seqs, stamp });
    await this.storage.put<ModelList>(MODEL_LIST_PREFIX + key, list);
    return true;
  }

  /**
   * Stores what the lease holder's ocx now holds. Its publishes can land out of order, so an older
   * version from the same process is ignored; a new process replaces whatever the last one held.
   */
  async reasoningMetadataCommit(bootId: string, kind: ReasoningMetadataKind, body: string, version: number): Promise<boolean> {
    if (!(await this.holdsLease(bootId))) return false;
    const stored = await this.storage.get<StoredReasoningMetadata>(REASONING_METADATA_PREFIX + kind);
    if (stored?.bootId === bootId && stored.version >= version) return true;
    await this.storage.put<StoredReasoningMetadata>(REASONING_METADATA_PREFIX + kind, { bootId, version, body });
    return true;
  }

  /** Each cache's JSON as last published (the text "null" when ocx has none), or undefined if never. */
  async reasoningMetadataRead(): Promise<Partial<Record<ReasoningMetadataKind, string>>> {
    const out: Partial<Record<ReasoningMetadataKind, string>> = {};
    for (const kind of REASONING_METADATA_KINDS) {
      const stored = await this.storage.get<StoredReasoningMetadata>(REASONING_METADATA_PREFIX + kind);
      if (typeof stored?.body === "string") out[kind] = stored.body;
    }
    return out;
  }

  /** Publishes from one process can land out of order; an older version from it is ignored. */
  async nativeOpenAiFactsCommit(bootId: string, facts: unknown, stamp: string, version: number): Promise<boolean> {
    if (!(await this.holdsLease(bootId))) return false;
    const stored = await this.storage.get<StoredNativeOpenAiFacts>(NATIVE_OPENAI_FACTS_KEY);
    if (stored?.bootId === bootId && stored.version >= version) return true;
    await this.storage.put<StoredNativeOpenAiFacts>(NATIVE_OPENAI_FACTS_KEY, { bootId, stamp, version, facts });
    return true;
  }

  async nativeOpenAiFactsRead(stamp: string): Promise<unknown> {
    const stored = await this.storage.get<StoredNativeOpenAiFacts>(NATIVE_OPENAI_FACTS_KEY);
    if (!stored || stored.stamp !== stamp) return undefined;
    const lease = await this.storage.get<Lease>(LEASE_KEY);
    if (lease && lease.bootId !== stored.bootId && this.now() - lease.heartbeatAt < LEASE_STALE_MS) return undefined;
    return stored.facts;
  }

  async clientRuntimeCommit(bootId: string, headers: Record<string, string>, stamp: string): Promise<boolean> {
    if (!(await this.holdsLease(bootId))) return false;
    await this.storage.put<StoredClientRuntime>(CLIENT_RUNTIME_KEY, { stamp, headers });
    return true;
  }

  /** The headers as published under `stamp`; undefined before this deployment's ocx has published. */
  async clientRuntimeRead(stamp: string): Promise<Record<string, string> | undefined> {
    const stored = await this.storage.get<StoredClientRuntime>(CLIENT_RUNTIME_KEY);
    return stored?.stamp === stamp ? stored.headers : undefined;
  }

  private async dropModelList(key: string): Promise<void> {
    await this.storage.delete(MODEL_LIST_META_PREFIX + key);
    await this.storage.delete(MODEL_LIST_PREFIX + key);
  }

  private async dropSkills(scope: string): Promise<void> {
    await this.storage.delete(SKILLS_META_PREFIX + scope);
    await this.storage.delete(SKILLS_BLOCK_PREFIX + scope);
  }
}

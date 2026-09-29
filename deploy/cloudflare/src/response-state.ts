// Continuations the Worker served: the replay entry a completed response leaves for
// previous_response_id (src/responses/state.ts's ResidentResponseState, as JSON), kept in the
// Durable Object because a Worker has no process to hold it and no disk to spill to. ocx's own
// store stays authoritative for responses IT completed — container writes never reach this
// keyspace — so a row here always names a Worker-served turn, read back either by the Worker
// (responseStateRead) or by the container prefetching before expandPreviousResponseInput
// (prefetchRemoteResponseState).
//
// Each entry is two rows, like the skills snapshot's (lease.ts): a small meta row so pruning and
// expiry never materialize bodies (1,000 entries at the value cap would not fit in a Durable
// Object's memory), and the serialized entry itself.
import type { LeaseStorage } from "./lease";

const RESPONSE_META_PREFIX = "ocx:resp-meta:";
const RESPONSE_BODY_PREFIX = "ocx:resp:";
// state.ts's RESPONSE_TTL_MS: a continuation outlives the turn that made it by exactly this long.
export const RESPONSE_STATE_TTL_MS = 24 * 60 * 60 * 1000;
// state.ts's MAX_STORED_RESPONSES.
export const MAX_RESPONSE_STATES = 1000;
// SQLite-backed DO storage caps a value at 2 MiB; the body row holds the serialized entry and
// nothing else, so it may fill almost all of that. Over-cap entries are dropped (the turn was
// still served — only its continuation is lost, as a container-side spill failure would lose it).
export const MAX_RESPONSE_STATE_BYTES = 2 * 1024 * 1024 - 4 * 1024;

type ResponseStateMeta = { expiresAt: number; createdAt: number };

/** The fields of a ResidentResponseState a row carries (everything `kind`/`sizeBytes` cover is DO-side). */
export type ResponseStateEntry = {
  createdAt: number;
  clientThreadId?: string;
  items: unknown[];
  providerOutputStart?: number;
  providers?: Record<string, unknown>;
};

/**
 * The entry as stored, or undefined when the value is not one: only whitelisted fields pass, so a
 * malformed write cannot smuggle keys a later expansion would read (expandWithReplayEntry reads
 * clientThreadId, items and providerOutputStart).
 */
export function normalizeResponseStateEntry(value: unknown): ResponseStateEntry | undefined {
  if (!value || typeof value !== "object" || Array.isArray(value)) return undefined;
  const record = value as Record<string, unknown>;
  if (!Number.isFinite(record.createdAt) || !Array.isArray(record.items)) return undefined;
  if (record.clientThreadId !== undefined && typeof record.clientThreadId !== "string") return undefined;
  if (record.providerOutputStart !== undefined && !Number.isSafeInteger(record.providerOutputStart)) return undefined;
  if (record.providers !== undefined
    && (!record.providers || typeof record.providers !== "object" || Array.isArray(record.providers))) return undefined;
  return {
    createdAt: record.createdAt as number,
    ...(record.clientThreadId !== undefined ? { clientThreadId: record.clientThreadId as string } : {}),
    items: record.items,
    ...(record.providerOutputStart !== undefined ? { providerOutputStart: record.providerOutputStart as number } : {}),
    ...(record.providers !== undefined ? { providers: record.providers as Record<string, unknown> } : {}),
  };
}

export class ResponseStateStore {
  constructor(private readonly storage: LeaseStorage, private readonly now: () => number = Date.now) {}

  /** The entry JSON, or undefined absent/expired/malformed; expiry deletes the pair lazily. */
  async getResponseState(id: string): Promise<string | undefined> {
    const meta = await this.storage.get<ResponseStateMeta>(RESPONSE_META_PREFIX + id);
    if (!meta) return undefined;
    if (this.now() >= meta.expiresAt) {
      await this.drop(id);
      return undefined;
    }
    return this.storage.get<string>(RESPONSE_BODY_PREFIX + id);
  }

  /**
   * Stores the entry for `id` under a 24 h TTL counted from its own createdAt, or returns false
   * when it does not fit a row — the caller still served the turn; only its continuation is lost.
   */
  async putResponseState(id: string, value: unknown): Promise<boolean> {
    const entry = normalizeResponseStateEntry(value);
    if (!entry || id.length === 0) return false;
    const body = JSON.stringify(entry);
    if (new TextEncoder().encode(body).byteLength > MAX_RESPONSE_STATE_BYTES) {
      console.error(`Response state ${id.slice(0, 32)} not stored: entry exceeds the Durable Object value cap`);
      return false;
    }
    await this.prune(id);
    // Meta first, as for skills: pruning and reset find bodies only through it, so a body must
    // never exist without its row.
    await this.storage.put<ResponseStateMeta>(RESPONSE_META_PREFIX + id, { expiresAt: entry.createdAt + RESPONSE_STATE_TTL_MS, createdAt: entry.createdAt });
    await this.storage.put(RESPONSE_BODY_PREFIX + id, body);
    return true;
  }

  /** Drops expired rows, then the soonest to expire until there is room for one more. */
  private async prune(keep: string): Promise<void> {
    const metas = await this.storage.list<ResponseStateMeta>({ prefix: RESPONSE_META_PREFIX, limit: MAX_RESPONSE_STATES + 50 });
    const now = this.now();
    const live: [string, number, number][] = [];
    for (const [key, meta] of metas) {
      const id = key.slice(RESPONSE_META_PREFIX.length);
      if (id === keep) continue;
      if (now >= meta.expiresAt) await this.drop(id);
      else live.push([id, meta.expiresAt, meta.createdAt]);
    }
    live.sort((a, b) => a[1] - b[1] || a[2] - b[2]);
    while (live.length >= MAX_RESPONSE_STATES) await this.drop(live.shift()![0]);
  }

  /** Every row goes on a reset: the container's own store was discarded with the same snapshot. */
  async discardAll(): Promise<void> {
    for (;;) {
      const metas = await this.storage.list<ResponseStateMeta>({ prefix: RESPONSE_META_PREFIX, limit: 1000 });
      if (metas.size === 0) break;
      for (const key of metas.keys()) await this.drop(key.slice(RESPONSE_META_PREFIX.length));
    }
  }

  private async drop(id: string): Promise<void> {
    await this.storage.delete(RESPONSE_META_PREFIX + id);
    await this.storage.delete(RESPONSE_BODY_PREFIX + id);
  }
}

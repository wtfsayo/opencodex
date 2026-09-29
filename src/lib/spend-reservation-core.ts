/**
 * The spend-reservation ledger's accounting core: every type, the journal contract, replay
 * and the ledger implementation itself. Separated from spend-reservation-ledger.ts, which
 * owns the FILE journal and the process singleton, because the Cloudflare deployment keeps
 * the same ledger over the hub Durable Object's SQL store (deploy/cloudflare/src/spend-ledger.ts)
 * and a container-side replica (durable-spend-ledger.ts), and neither can import node:fs.
 *
 * This module's import boundary is load-bearing: tests/service/cloudflare-worker-native.test.ts
 * walks the Worker's whole import graph, so nothing here may touch node:fs, bun:* or the
 * state-directory owner module. `node:crypto` alone is allowed -- the Workers runtime serves
 * it under nodejs_compat (wrangler.jsonc) and the deploy bundle declares it in
 * deploy/cloudflare/src/node-compat.d.ts.
 *
 * The guarantees are the ledger's, and they are stated in spend-reservation-ledger.ts's
 * header: idempotent send ids, durability before admission, validating replay, bounded
 * retention, and nothing identifying on disk -- every id written to a journal is the salted
 * alias `aliasFor` computes, so a journal on either store holds no raw credential, header or
 * pool name.
 */

import { createHash } from "node:crypto";
// Type-only, so it is erased before this module has a runtime import graph at all. The
// config SHAPE is what this file needs; the config loader is what stays out.
import type { OcxSpendConfig, OcxSpendScopeConfig } from "../types/config";

export type SpendLedgerOwnerErrorCode =
  | "SPEND_LEDGER_OWNER_BUSY"
  | "SPEND_LEDGER_OWNER_UNAVAILABLE"
  | "SPEND_LEDGER_OWNER_HOME_CONFLICT"
  | "SPEND_LEDGER_OWNER_NOT_HELD";

/**
 * The ownership and durability failure every ledger store reports with. It lives here
 * because the core's journal-write path must recognise it -- an ownership error ABORTS an
 * operation rather than counting as a failed write -- and spend-ledger-owner.ts, which
 * raised it first, cannot be imported from here (it pulls in bun:sqlite).
 */
export class SpendLedgerOwnerError extends Error {
  constructor(readonly code: SpendLedgerOwnerErrorCode, message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "SpendLedgerOwnerError";
  }
}

export type SpendScope = "root" | "identity" | "pool";

export interface SpendScopeLimit {
  /**
   * Approved token ceiling for the scope. Undefined means OBSERVE ONLY: spend is still
   * accounted and reported, but nothing is refused. That is the unconfigured default --
   * an install that never opted in keeps the count caps and is not newly refused.
   */
  readonly maxTokens?: number;
}

export interface SpendReservationPolicy {
  readonly root: SpendScopeLimit;
  readonly identity: SpendScopeLimit;
  readonly pool: SpendScopeLimit;
  /**
   * How long a dormant scope's accounting is retained. A scope may be dropped only when it
   * is BOTH inactive (no open reservation) AND not exhausted inside this window; dropping
   * an exhausted scope would hand it a fresh allowance on next use.
   */
  readonly retentionMs: number;
  /**
   * Hard ceiling on tracked scopes. Retention alone bounds nothing: a caller minting a fresh
   * root id per request fills the map long before the window elapses. At the ceiling the
   * ledger evicts the oldest scope that is safe to forget -- idle, under its limit, past
   * retention -- and if there is none it REFUSES the new scope. Refusing is the only answer
   * left: the alternative is evicting an exhausted scope, which hands it a fresh allowance.
   */
  readonly maxTrackedScopes?: number;
  /** Hard ceiling on remembered send ids, with the same evict-or-refuse rule. */
  readonly maxTrackedSends?: number;
  /**
   * Journal records after which the file is compacted into a single checkpoint. Without
   * this the file grows forever even while the in-memory maps stay bounded, and replay
   * resurrects every entry cleanup removed.
   */
  readonly compactAfterRecords?: number;
}

const DEFAULT_MAX_TRACKED_SCOPES = 4_096;
const DEFAULT_MAX_TRACKED_SENDS = 16_384;
const DEFAULT_COMPACT_AFTER_RECORDS = 8_192;

/**
 * Unconfigured default: every limit undefined, so token accounting runs in observe-only
 * mode and the count caps remain the only enforcement. Real numbers belong behind
 * explicit operator configuration.
 */
export const DEFAULT_SPEND_RESERVATION_POLICY: SpendReservationPolicy = {
  root: {},
  identity: {},
  pool: {},
  retentionMs: 7 * 24 * 60 * 60_000,
};

export interface SpendScopes {
  readonly rootId?: string;
  readonly identityId?: string;
  readonly poolId?: string;
}

export interface SpendUsage {
  readonly inputTokens: number;
  readonly outputTokens: number;
}

export interface SpendReservationRequest {
  /** Stable id of the physical send. Settlement is idempotent on this key. */
  readonly sendId: string;
  readonly scopes: SpendScopes;
  readonly inputTokens: number;
  /** Enforceable output ceiling -- max_output_tokens or the model's documented cap. */
  readonly outputCeilingTokens: number;
  readonly at?: number;
  /**
   * This send has ALREADY left for upstream and is being recorded rather than admitted.
   *
   * Some transports report their physical sends after the fact -- the passthrough ladder
   * reports through `onSendsConsumed`, and an adapter's inner retries are counted when they
   * finish. For those, a ceiling cannot refuse anything: the tokens are spent. Refusing to
   * BOOK them is the worse answer, and it is not hypothetical -- it is a fixpoint. The send
   * that would cross the ceiling gets dropped from the total, the total stays just under the
   * limit forever, the scope never reads as exhausted, and the ceiling never fires again for
   * any request. So a recorded send skips the limit check and takes the scope over its
   * ceiling, which is what makes the NEXT request refusable.
   *
   * It skips the durability refusal for the same reason: a journal that could not be written
   * is a reason to report degradation, never a reason to forget spend that really happened.
   * Identity, capacity and journal-integrity denials still apply -- those say the ledger
   * cannot account for the send at all, which no flag here can change.
   */
  readonly alreadySent?: boolean;
}

/**
 * Why a reservation was refused. Every member refuses a DISPATCH: none of them is an
 * "already fine, carry on" answer, because that is precisely how a duplicate send id used
 * to buy an unlimited number of physical sends while the scope totals never moved.
 */
export type SpendDenial =
  | {
      readonly reason: "spend-limit-exceeded";
      readonly scope: SpendScope;
      readonly scopeId: string;
      readonly limit: number;
      readonly projected: number;
    }
  /** This send id is already known -- open, settled, lost or abandoned. */
  | { readonly reason: "duplicate-send-id"; readonly sendId: string }
  /** The reserve record could not be written, and a configured limit needs it to survive. */
  | { readonly reason: "reserve-not-durable"; readonly sendId: string }
  /** Replay rejected records mid-file, so no scope total can be proven complete. */
  | { readonly reason: "journal-corrupt"; readonly corruptRecords: number }
  /** Tracking is full and nothing may be forgotten safely. */
  | { readonly reason: "tracking-capacity-exhausted"; readonly scope?: SpendScope };

export type SpendReservationDecision =
  | {
      readonly reserved: true;
      readonly sendId: string;
      readonly tokens: number;
      /**
       * False only in observe-only mode, where the reservation was admitted although its
       * journal record did not reach storage. A restart will not remember this spend; the flag
       * is how a caller learns that instead of discovering it after the fact.
       */
      readonly durable: boolean;
    }
  | { readonly reserved: false; readonly denial: SpendDenial };

interface ScopeState {
  settled: number;
  reserved: number;
  unresolved: number;
  lastSeenAt: number;
}

/**
 * `open` means admitted but not yet handed to a transport: it may still be abandoned for
 * free. `dispatched` means bytes left for upstream, so from there a missing usage frame is
 * unresolved SPEND rather than a release -- it may have been billed. Only a dispatched send
 * can become `lost`; only an undispatched one can become `abandoned`.
 */
type ReservationStatus = "open" | "dispatched" | "settled" | "lost" | "abandoned";

interface ScopeRef {
  readonly scope: SpendScope;
  readonly alias: string;
}

interface Reservation {
  readonly targets: readonly ScopeRef[];
  readonly tokens: number;
  status: ReservationStatus;
  readonly at: number;
  /** When the status last changed; drives eviction of resolved entries. */
  resolvedAt: number;
}

/**
 * Journal shape. Every id on disk is a salted alias, never a root header value, credential
 * id or pool name. `forget` and `drop` are the tombstones that make bounded cleanup
 * durable -- without them replay rebuilds exactly what cleanup removed -- and `checkpoint`
 * is a whole-state snapshot that lets the file be compacted instead of growing forever.
 */
export type SpendJournalRecord =
  | { v: 1; kind: "reserve"; send: string; targets: ScopeRef[]; tokens: number; at: number }
  | { v: 1; kind: "dispatch"; send: string; at: number }
  | { v: 1; kind: "settle"; send: string; tokens: number; at: number }
  | { v: 1; kind: "lost"; send: string; at: number }
  | { v: 1; kind: "abandon"; send: string; at: number }
  | { v: 1; kind: "forget"; send: string; at: number }
  | { v: 1; kind: "drop"; scope: SpendScope; alias: string; at: number }
  | {
      v: 1;
      kind: "checkpoint";
      at: number;
      scopes: { scope: SpendScope; alias: string; settled: number; unresolved: number; seenAt: number }[];
      sends: { send: string; status: ReservationStatus; targets: ScopeRef[]; tokens: number; at: number; resolvedAt: number }[];
    };

const isCountable = (value: unknown): value is number =>
  typeof value === "number" && Number.isFinite(value) && value >= 0;

const isAlias = (value: unknown): value is string =>
  typeof value === "string" && value.length > 0 && value.length <= 256;

const isScopeName = (value: unknown): value is SpendScope =>
  value === "root" || value === "identity" || value === "pool";

const isStatus = (value: unknown): value is ReservationStatus =>
  value === "open" || value === "dispatched" || value === "settled"
  || value === "lost" || value === "abandoned";

const parseTargets = (value: unknown): ScopeRef[] | undefined => {
  if (!Array.isArray(value) || value.length > 3) return undefined;
  const targets: ScopeRef[] = [];
  for (const entry of value) {
    if (typeof entry !== "object" || entry === null) return undefined;
    const { scope, alias } = entry as { scope?: unknown; alias?: unknown };
    if (!isScopeName(scope) || !isAlias(alias)) return undefined;
    targets.push({ scope, alias });
  }
  return targets;
};

/**
 * Validate one journal line into a record, or reject it.
 *
 * Exported because this is the boundary where a hostile or damaged file meets the accounting:
 * `JSON.parse(line) as JournalRecord` type-asserts a lie, and a bare `null` line or a
 * `{"v":1,"kind":"reserve"}` with no fields crashed the rebuild rather than being rejected.
 * Every field is checked, including that numbers are finite and non-negative.
 */
export function parseSpendJournalRecord(line: string): SpendJournalRecord | undefined {
  let raw: unknown;
  try {
    raw = JSON.parse(line);
  } catch {
    return undefined;
  }
  if (typeof raw !== "object" || raw === null || Array.isArray(raw)) return undefined;
  const record = raw as Record<string, unknown>;
  if (record.v !== 1) return undefined;
  if (!isCountable(record.at)) return undefined;
  const at = record.at;
  switch (record.kind) {
    case "reserve": {
      const targets = parseTargets(record.targets);
      if (!isAlias(record.send) || targets === undefined || !isCountable(record.tokens)) return undefined;
      return { v: 1, kind: "reserve", send: record.send, targets, tokens: record.tokens, at };
    }
    case "settle":
      if (!isAlias(record.send) || !isCountable(record.tokens)) return undefined;
      return { v: 1, kind: "settle", send: record.send, tokens: record.tokens, at };
    case "dispatch":
      if (!isAlias(record.send)) return undefined;
      return { v: 1, kind: "dispatch", send: record.send, at };
    case "lost":
      if (!isAlias(record.send)) return undefined;
      return { v: 1, kind: "lost", send: record.send, at };
    case "abandon":
      if (!isAlias(record.send)) return undefined;
      return { v: 1, kind: "abandon", send: record.send, at };
    case "forget":
      if (!isAlias(record.send)) return undefined;
      return { v: 1, kind: "forget", send: record.send, at };
    case "drop":
      if (!isScopeName(record.scope) || !isAlias(record.alias)) return undefined;
      return { v: 1, kind: "drop", scope: record.scope, alias: record.alias, at };
    case "checkpoint": {
      if (!Array.isArray(record.scopes) || !Array.isArray(record.sends)) return undefined;
      const scopes: { scope: SpendScope; alias: string; settled: number; unresolved: number; seenAt: number }[] = [];
      for (const entry of record.scopes) {
        if (typeof entry !== "object" || entry === null) return undefined;
        const e = entry as Record<string, unknown>;
        if (!isScopeName(e.scope) || !isAlias(e.alias)) return undefined;
        if (!isCountable(e.settled) || !isCountable(e.unresolved) || !isCountable(e.seenAt)) return undefined;
        scopes.push({ scope: e.scope, alias: e.alias, settled: e.settled, unresolved: e.unresolved, seenAt: e.seenAt });
      }
      const sends: { send: string; status: ReservationStatus; targets: ScopeRef[]; tokens: number; at: number; resolvedAt: number }[] = [];
      for (const entry of record.sends) {
        if (typeof entry !== "object" || entry === null) return undefined;
        const e = entry as Record<string, unknown>;
        const targets = parseTargets(e.targets);
        if (!isAlias(e.send) || !isStatus(e.status) || targets === undefined) return undefined;
        if (!isCountable(e.tokens) || !isCountable(e.at) || !isCountable(e.resolvedAt)) return undefined;
        sends.push({ send: e.send, status: e.status, targets, tokens: e.tokens, at: e.at, resolvedAt: e.resolvedAt });
      }
      return { v: 1, kind: "checkpoint", at, scopes, sends };
    }
    default:
      return undefined;
  }
}

/**
 * Append-mostly persistence. `read` returns raw lines so replay can tell a torn TAIL write
 * from corruption earlier in the file; only the former is safe to drop quietly. `append`
 * THROWS when the record did not reach storage -- that signal is what lets admission refuse
 * rather than admit a request a restart would forget. `rewrite` is optional: a store that
 * cannot replace its contents atomically simply never compacts.
 */
export interface SpendJournal {
  read(): string[];
  append(line: string): void;
  rewrite?(lines: string[]): void;
}

export interface ScopeSpendSnapshot {
  readonly settled: number;
  readonly reserved: number;
  readonly unresolved: number;
  readonly exhausted: boolean;
}

export interface SpendReservationLedger {
  reserve(request: SpendReservationRequest): SpendReservationDecision;
  /**
   * The send left for upstream. Until this is called the reservation may be abandoned for
   * free; after it, a missing usage frame becomes unresolved spend. Returns false when the
   * send is unknown or no longer open.
   */
  markDispatched(sendId: string): boolean;
  /**
   * The send never happened -- local validation, routing, or a refusal before any byte left
   * this process. The reservation is RELEASED and books nothing, because inventing debt the
   * account never incurred is its own way of breaking the budget. Refused once the send is
   * dispatched: from there only settle or markLost is honest.
   */
  abandon(sendId: string): boolean;
  /**
   * Settle with real usage. Returns false when the send is unknown or already resolved --
   * double settlement is as wrong as none, so a repeat call changes nothing.
   */
  settle(sendId: string, usage: SpendUsage): boolean;
  /**
   * Usage never arrived. The reservation moves to unresolved spend -- it may have been
   * billed -- rather than being released. Idempotent on the same key as settle.
   */
  markLost(sendId: string): boolean;
  /**
   * Fold one journal record written by a PEER into this ledger's maps, exactly as replay
   * applies it. The Durable Object's ledger takes the container's appended records this way:
   * the container's replica journalises them itself, so the ledger cannot append them again,
   * but the accounting still has to move.
   */
  importRecord(record: SpendJournalRecord): void;
  snapshot(scope: SpendScope, scopeId: string): ScopeSpendSnapshot | undefined;
  exhausted(scope: SpendScope, scopeId: string): boolean;
  /**
   * Drop dormant scopes per the retention rule in SpendReservationPolicy. Cleanup also runs
   * automatically on every reservation, so nothing depends on a caller remembering this.
   */
  prune(now?: number): void;
  /** Whether this send id is already known, and therefore refused. */
  knows(sendId: string): boolean;
  /**
   * Replace the live policy.
   *
   * Every figure already accounted survives: raising, lowering or clearing a ceiling changes
   * what is REFUSED from here on and never what was spent. Rebuilding the ledger instead
   * would replay the journal into a second set of maps while the first still holds this
   * process's open reservations, and the two would then disagree about what is in flight.
   */
  reconfigure(next: SpendReservationPolicy): void;
  /**
   * The policy in force. A live read, not a copy: a caller that formats a refusal has to name
   * the ceiling this ledger would enforce on the NEXT request, not the one it was built with.
   */
  readonly policy: SpendReservationPolicy;
  /** Journal writes that failed; a nonzero count means durability is degraded. */
  readonly persistFailures: number;
  /**
   * Records replay rejected in the MIDDLE of the journal. Nonzero means no scope total can
   * be proven complete, so configured limits refuse rather than undercount.
   */
  readonly corruptRecords: number;
  /** True when durability is degraded in either direction: failed writes or a corrupt file. */
  readonly degraded: boolean;
}

const scopeKey = (scope: SpendScope, alias: string): string => scope + "\0" + alias;

const sanitizeTokens = (value: number): number =>
  Number.isFinite(value) ? Math.max(0, Math.trunc(value)) : 0;

export function createSpendReservationLedger(options: {
  readonly journal?: SpendJournal;
  readonly policy?: SpendReservationPolicy;
  readonly now?: () => number;
  /**
   * Per-install alias salt. Production passes the file-backed value from
   * `loadOrCreateSpendLedgerSalt`; an empty default is for in-memory journals, which have
   * no file anyone could correlate.
   */
  readonly salt?: string;
  /**
   * Shared production ledgers prove their exact ownership before reading or changing
   * accounting. Identity, not just the directory name: a handle kept across a release and a
   * reacquire describes a journal another writer may have changed in between.
   */
  readonly assertOwnedAccounting?: () => void;
  /**
   * Default true: replay marks every reservation still live as unresolved spend, because the
   * process that owned it is gone and its usage frame can never arrive. A ledger built over a
   * JOURNAL REPLICA (the container's view of the Cloudflare Durable Object's journal) passes
   * false: the reservations it replays belong to writers still alive — the DO itself — and
   * marking them lost would book send ids the authority still considers open.
   */
  readonly reconcileReplayedSends?: boolean;
} = {}): SpendReservationLedger {
  // Mutable because the ceilings are operator configuration, and configuration is reloadable.
  // The three bounds below are read through functions for the same reason: a value captured
  // at construction would answer for the policy this ledger was BUILT with, and an operator
  // who raised a bound would keep the old one until the process restarted.
  let policy = options.policy ?? DEFAULT_SPEND_RESERVATION_POLICY;
  const journal = options.journal;
  const now = options.now ?? (() => Date.now());
  const salt = options.salt ?? "";
  const assertOwnedAccounting = options.assertOwnedAccounting;
  const maxTrackedScopes = (): number => policy.maxTrackedScopes ?? DEFAULT_MAX_TRACKED_SCOPES;
  const maxTrackedSends = (): number => policy.maxTrackedSends ?? DEFAULT_MAX_TRACKED_SENDS;
  const compactAfterRecords = (): number => policy.compactAfterRecords ?? DEFAULT_COMPACT_AFTER_RECORDS;
  const scopes = new Map<string, ScopeState>();
  const reservations = new Map<string, Reservation>();
  let persistFailures = 0;
  let corruptRecords = 0;
  let recordsOnDisk = 0;

  /**
   * Salted alias for one identifier. The raw value -- a client-supplied root header, a
   * credential id, a pool name -- never leaves this function, so nothing identifying is
   * written to disk or held in a map key.
   */
  const aliasFor = (kind: SpendScope | "send", id: string): string =>
    createHash("sha256").update(salt).update("\0").update(kind).update("\0").update(id)
      .digest("hex").slice(0, 32);

  const scopeState = (scope: SpendScope, alias: string): ScopeState => {
    const key = scopeKey(scope, alias);
    let state = scopes.get(key);
    if (!state) {
      state = { settled: 0, reserved: 0, unresolved: 0, lastSeenAt: 0 };
      scopes.set(key, state);
    }
    return state;
  };

  const limitFor = (scope: SpendScope): number | undefined => policy[scope].maxTokens;

  const isExhausted = (scope: SpendScope, state: ScopeState): boolean => {
    const limit = limitFor(scope);
    return limit !== undefined && state.settled + state.reserved + state.unresolved >= limit;
  };

  /** The scopes a request touches, as aliases. Creates no state: a refusal must leave none. */
  const refsFor = (targets: SpendScopes): ScopeRef[] => {
    const refs: ScopeRef[] = [];
    if (targets.rootId !== undefined) refs.push({ scope: "root", alias: aliasFor("root", targets.rootId) });
    if (targets.identityId !== undefined) refs.push({ scope: "identity", alias: aliasFor("identity", targets.identityId) });
    if (targets.poolId !== undefined) refs.push({ scope: "pool", alias: aliasFor("pool", targets.poolId) });
    return refs;
  };

  /**
   * Returns whether the record reached storage. With no journal there is nothing to fail,
   * and the caller's durability question is vacuously satisfied.
   */
  const append = (record: SpendJournalRecord): boolean => {
    if (!journal) return true;
    try {
      journal.append(JSON.stringify(record));
      recordsOnDisk += 1;
      return true;
    } catch (error) {
      if (error instanceof SpendLedgerOwnerError) throw error;
      // In-memory state still bounds this process; the counter is how a caller learns the
      // restart guarantee degraded instead of discovering it after the fact.
      persistFailures += 1;
      return false;
    }
  };

  const applyReserve = (send: string, targets: readonly ScopeRef[], tokens: number, at: number): void => {
    if (reservations.has(send)) return;
    reservations.set(send, { targets, tokens, status: "open", at, resolvedAt: at });
    for (const ref of targets) {
      const state = scopeState(ref.scope, ref.alias);
      state.reserved += tokens;
      state.lastSeenAt = Math.max(state.lastSeenAt, at);
    }
  };

  const isLive = (status: ReservationStatus): boolean => status === "open" || status === "dispatched";

  /**
   * Resolve a live reservation. `settled` books the real figure, `lost` keeps the whole
   * reservation as unresolved spend because it may have been billed, and `abandoned`
   * releases it because no byte ever left this process.
   */
  const applyResolve = (send: string, outcome: "settled" | "lost" | "abandoned", tokens: number, at: number): void => {
    const reservation = reservations.get(send);
    if (!reservation || !isLive(reservation.status)) return;
    reservation.status = outcome;
    reservation.resolvedAt = at;
    for (const ref of reservation.targets) {
      const state = scopeState(ref.scope, ref.alias);
      state.reserved = Math.max(0, state.reserved - reservation.tokens);
      if (outcome === "lost") state.unresolved += reservation.tokens;
      else if (outcome === "settled") state.settled += tokens;
      state.lastSeenAt = Math.max(state.lastSeenAt, at);
    }
  };

  const applyDispatch = (send: string, at: number): void => {
    const reservation = reservations.get(send);
    if (!reservation || reservation.status !== "open") return;
    reservation.status = "dispatched";
    reservation.resolvedAt = at;
  };

  /** Tombstone replay: the entry is gone, so a later reuse of the id books a fresh charge. */
  const applyForget = (send: string): void => {
    const reservation = reservations.get(send);
    if (!reservation || isLive(reservation.status)) return;
    reservations.delete(send);
  };

  const applyDrop = (scope: SpendScope, alias: string): void => {
    const state = scopes.get(scopeKey(scope, alias));
    if (!state || state.reserved > 0) return;
    scopes.delete(scopeKey(scope, alias));
  };

  const applyCheckpoint = (record: Extract<SpendJournalRecord, { kind: "checkpoint" }>): void => {
    scopes.clear();
    reservations.clear();
    for (const entry of record.scopes) {
      scopes.set(scopeKey(entry.scope, entry.alias), {
        settled: entry.settled,
        reserved: 0,
        unresolved: entry.unresolved,
        lastSeenAt: entry.seenAt,
      });
    }
    for (const entry of record.sends) {
      // `reserved` is rebuilt from the live entries rather than trusted from the snapshot,
      // so the two can never disagree about the same tokens.
      if (isLive(entry.status)) {
        applyReserve(entry.send, entry.targets, entry.tokens, entry.at);
        if (entry.status === "dispatched") applyDispatch(entry.send, entry.resolvedAt);
        continue;
      }
      reservations.set(entry.send, {
        targets: entry.targets,
        tokens: entry.tokens,
        status: entry.status,
        at: entry.at,
        resolvedAt: entry.resolvedAt,
      });
    }
  };

  /** One record applied the way replay applies it, for a journal row a peer already wrote. */
  const applyRecord = (record: SpendJournalRecord): void => {
    switch (record.kind) {
      case "reserve": applyReserve(record.send, record.targets, sanitizeTokens(record.tokens), record.at); break;
      case "dispatch": applyDispatch(record.send, record.at); break;
      case "settle": applyResolve(record.send, "settled", sanitizeTokens(record.tokens), record.at); break;
      case "lost": applyResolve(record.send, "lost", 0, record.at); break;
      case "abandon": applyResolve(record.send, "abandoned", 0, record.at); break;
      case "forget": applyForget(record.send); break;
      case "drop": applyDrop(record.scope, record.alias); break;
      case "checkpoint": applyCheckpoint(record); break;
    }
  };

  // Rebuild from the journal before serving: an exhausted scope must still be exhausted
  // after a restart, which is the whole reason this store exists.
  if (journal) {
    const lines = journal.read();
    recordsOnDisk = lines.length;
    for (let index = 0; index < lines.length; index += 1) {
      const line = lines[index] as string;
      const record = parseSpendJournalRecord(line);
      if (!record) {
        // A rejected FINAL line is a torn tail write -- the process died between the write
        // and its newline -- and is dropped quietly, because that record never completed and
        // therefore never authorised anything. A rejected line ANYWHERE ELSE is different:
        // the records after it did complete, so skipping it silently undercounts a scope and
        // hands back budget. It is counted, and a configured limit refuses on it below.
        if (index < lines.length - 1) corruptRecords += 1;
        continue;
      }
      applyRecord(record);
    }
    // A reservation that survived replay has no owner left. The process that made it is gone,
    // so nothing in this one can ever settle it, and leaving it live means the send stays
    // pending forever against a scope that can never resolve it. Deleting the entry is not the
    // alternative either: that would hand the same send id a second reservation.
    //
    // Both live states resolve to UNRESOLVED, including an undispatched one. The tempting
    // distinction -- open never reached the wire, so give its tokens back -- assumes the
    // journal is complete up to the crash, and the torn-tail handling above says it is not: a
    // send can dispatch and die before its dispatch record lands. Abandoning that reservation
    // returns tokens for a send that may have been billed, and worse, it RESETS a ceiling that
    // had already fired. An exhausted scope staying exhausted across a restart is the whole
    // reason this store is on disk.
    // Skipped only by a replica whose journal's writer outlives this replay
    // (reconcileReplayedSends): its open reservations belong to that writer, not a dead one.
    if (options.reconcileReplayedSends !== false) {
      const reconciledAt = now();
      for (const [send, reservation] of reservations) {
        if (!isLive(reservation.status)) continue;
        applyResolve(send, "lost", 0, reconciledAt);
        append({ v: 1, kind: "lost", send, at: reconciledAt });
      }
    }
  }

  /**
   * Bounded cleanup. It runs before every admission, so nothing depends on a caller
   * remembering `prune()` -- the first draft exported one and no production path called it.
   * Every removal writes a tombstone: without one, replay rebuilds precisely what cleanup
   * removed and the file keeps growing while the maps look bounded.
   *
   * `force` is the at-capacity pass. It ignores the retention window but never the safety
   * rule: an ACTIVE or EXHAUSTED scope is not a candidate at any pressure, because dropping
   * one hands it a fresh allowance under the same id. When that leaves nothing to remove,
   * the caller refuses admission rather than making room by forgetting a spent scope.
   */
  const evictScopes = (at: number, force: boolean): number => {
    const cutoff = at - policy.retentionMs;
    const candidates: { key: string; scope: SpendScope; alias: string; seenAt: number }[] = [];
    for (const [key, state] of scopes) {
      const separator = key.indexOf("\0");
      const scope = key.slice(0, separator) as SpendScope;
      if (state.reserved > 0) continue;
      if (isExhausted(scope, state)) continue;
      if (!force && state.lastSeenAt >= cutoff) continue;
      candidates.push({ key, scope, alias: key.slice(separator + 1), seenAt: state.lastSeenAt });
    }
    if (force) {
      candidates.sort((a, b) => a.seenAt - b.seenAt);
      candidates.length = Math.min(candidates.length, 1);
    }
    for (const candidate of candidates) {
      scopes.delete(candidate.key);
      append({ v: 1, kind: "drop", scope: candidate.scope, alias: candidate.alias, at });
    }
    return candidates.length;
  };

  /**
   * Forget resolved send ids. A forgotten id is forgotten COMPLETELY: reusing it later books
   * a fresh reservation against every scope, which is conservative. The state this must never
   * produce is the middle one -- an id the ledger recognises but charges nothing for.
   */
  const evictSends = (at: number, force: boolean): number => {
    const cutoff = at - policy.retentionMs;
    const candidates: { send: string; resolvedAt: number }[] = [];
    for (const [send, reservation] of reservations) {
      if (isLive(reservation.status)) continue;
      if (!force && reservation.resolvedAt >= cutoff) continue;
      candidates.push({ send, resolvedAt: reservation.resolvedAt });
    }
    if (force) {
      candidates.sort((a, b) => a.resolvedAt - b.resolvedAt);
      candidates.length = Math.min(candidates.length, 1);
    }
    for (const candidate of candidates) {
      reservations.delete(candidate.send);
      append({ v: 1, kind: "forget", send: candidate.send, at });
    }
    return candidates.length;
  };

  /**
   * Replace the journal with a single checkpoint once it has grown past its record budget.
   * Bounded maps are not enough on their own: the file behind them is what replay reads, and
   * an uncompacted file grows forever on unique root and send ids.
   */
  const compact = (at: number): void => {
    const rewrite = journal?.rewrite;
    if (!journal || !rewrite || recordsOnDisk < compactAfterRecords()) return;
    const checkpoint: SpendJournalRecord = {
      v: 1,
      kind: "checkpoint",
      at,
      scopes: [...scopes].map(([key, state]) => {
        const separator = key.indexOf("\0");
        return {
          scope: key.slice(0, separator) as SpendScope,
          alias: key.slice(separator + 1),
          settled: state.settled,
          unresolved: state.unresolved,
          seenAt: state.lastSeenAt,
        };
      }),
      sends: [...reservations].map(([send, reservation]) => ({
        send,
        status: reservation.status,
        targets: [...reservation.targets],
        tokens: reservation.tokens,
        at: reservation.at,
        resolvedAt: reservation.resolvedAt,
      })),
    };
    try {
      rewrite.call(journal, [JSON.stringify(checkpoint)]);
      recordsOnDisk = 1;
    } catch (error) {
      if (error instanceof SpendLedgerOwnerError) throw error;
      // Compaction is maintenance, not accounting: a failed rewrite leaves the previous
      // journal intact and every figure in it still replayable.
      persistFailures += 1;
    }
  };

  /** The denial when tracking cannot fit this request, or undefined when it can. */
  const makeRoom = (refs: readonly ScopeRef[], at: number): SpendDenial | undefined => {
    evictSends(at, false);
    evictScopes(at, false);
    while (reservations.size >= maxTrackedSends()) {
      if (evictSends(at, true) === 0) return { reason: "tracking-capacity-exhausted" };
    }
    let fresh = 0;
    for (const ref of refs) if (!scopes.has(scopeKey(ref.scope, ref.alias))) fresh += 1;
    while (scopes.size + fresh > maxTrackedScopes()) {
      if (evictScopes(at, true) === 0) {
        return { reason: "tracking-capacity-exhausted", scope: refs[0]?.scope };
      }
    }
    return undefined;
  };

  return {
    // Every figure this ledger reports describes a journal it must still own. Reporting one
    // after ownership ended is the same error as writing then, with a quieter symptom.
    get persistFailures() { assertOwnedAccounting?.(); return persistFailures; },
    get corruptRecords() { assertOwnedAccounting?.(); return corruptRecords; },
    get degraded() { assertOwnedAccounting?.(); return persistFailures > 0 || corruptRecords > 0; },
    get policy() { assertOwnedAccounting?.(); return policy; },

    reserve(request: SpendReservationRequest): SpendReservationDecision {
      assertOwnedAccounting?.();
      const tokens = sanitizeTokens(request.inputTokens) + sanitizeTokens(request.outputCeilingTokens);
      const at = request.at ?? now();
      const send = aliasFor("send", request.sendId);
      const refs = refsFor(request.scopes);
      const enforced = refs.some((ref) => limitFor(ref.scope) !== undefined);

      // A send id this ledger already knows is REFUSED. Returning success while booking
      // nothing -- the old behaviour -- let one id authorise an unlimited number of physical
      // sends with the scope totals never moving.
      if (reservations.has(send)) {
        return { reserved: false, denial: { reason: "duplicate-send-id", sendId: request.sendId } };
      }
      // Replay could not prove these totals are complete, so a configured ceiling cannot be
      // enforced on them. Observe-only accounting continues and reports the degradation.
      if (enforced && corruptRecords > 0) {
        return { reserved: false, denial: { reason: "journal-corrupt", corruptRecords } };
      }
      const capacity = makeRoom(refs, at);
      if (capacity) return { reserved: false, denial: capacity };

      // Check every scope before mutating any: a refusal must not leave a partial
      // reservation booked on the scopes that would have passed. Reading state without
      // creating it matters here -- a denied request must not leave a tracked scope behind.
      for (const ref of refs) {
        // A recorded send has no limit to fail: it already happened, and the point of booking
        // it is to let the total go OVER the ceiling so the next request can be refused.
        const limit = request.alreadySent === true ? undefined : limitFor(ref.scope);
        if (limit === undefined) continue;
        const state = scopes.get(scopeKey(ref.scope, ref.alias));
        const projected = (state ? state.settled + state.reserved + state.unresolved : 0) + tokens;
        if (projected > limit) {
          const scopeId = ref.scope === "root"
            ? request.scopes.rootId
            : ref.scope === "identity" ? request.scopes.identityId : request.scopes.poolId;
          return {
            reserved: false,
            denial: { reason: "spend-limit-exceeded", scope: ref.scope, scopeId: scopeId ?? "", limit, projected },
          };
        }
      }

      // Durability BEFORE admission. The record goes to disk first, and under a configured
      // limit a failed write refuses the request rather than admitting one that a restart
      // would forget -- which is exactly the disk-full and permission case durability is for.
      const durable = append({ v: 1, kind: "reserve", send, targets: refs, tokens, at });
      if (!durable && enforced && request.alreadySent !== true) {
        return { reserved: false, denial: { reason: "reserve-not-durable", sendId: request.sendId } };
      }
      applyReserve(send, refs, tokens, at);
      compact(at);
      return { reserved: true, sendId: request.sendId, tokens, durable };
    },

    markDispatched(sendId: string): boolean {
      assertOwnedAccounting?.();
      const send = aliasFor("send", sendId);
      const reservation = reservations.get(send);
      if (!reservation || reservation.status !== "open") return false;
      const at = now();
      applyDispatch(send, at);
      append({ v: 1, kind: "dispatch", send, at });
      return true;
    },

    abandon(sendId: string): boolean {
      assertOwnedAccounting?.();
      const send = aliasFor("send", sendId);
      const reservation = reservations.get(send);
      // Only an UNDISPATCHED reservation may be released for free. Once bytes have left for
      // upstream the tokens may already be billed, so the caller owes settle or markLost.
      if (!reservation || reservation.status !== "open") return false;
      const at = now();
      applyResolve(send, "abandoned", 0, at);
      append({ v: 1, kind: "abandon", send, at });
      return true;
    },

    settle(sendId: string, usage: SpendUsage): boolean {
      assertOwnedAccounting?.();
      const send = aliasFor("send", sendId);
      const reservation = reservations.get(send);
      if (!reservation || !isLive(reservation.status)) return false;
      const tokens = sanitizeTokens(usage.inputTokens) + sanitizeTokens(usage.outputTokens);
      const at = now();
      applyResolve(send, "settled", tokens, at);
      append({ v: 1, kind: "settle", send, tokens, at });
      return true;
    },

    markLost(sendId: string): boolean {
      assertOwnedAccounting?.();
      const send = aliasFor("send", sendId);
      const reservation = reservations.get(send);
      if (!reservation || !isLive(reservation.status)) return false;
      const at = now();
      applyResolve(send, "lost", 0, at);
      append({ v: 1, kind: "lost", send, at });
      return true;
    },

    importRecord(record: SpendJournalRecord): void {
      assertOwnedAccounting?.();
      // The caller journalised the line itself; the count still tracks what the journal
      // holds, so compaction fires on the same schedule replay and appends follow.
      recordsOnDisk += 1;
      applyRecord(record);
    },

    knows(sendId: string): boolean {
      assertOwnedAccounting?.();
      return reservations.has(aliasFor("send", sendId));
    },

    snapshot(scope: SpendScope, scopeId: string): ScopeSpendSnapshot | undefined {
      // Reading accounting from a handle whose ownership has ended is as wrong as writing it:
      // the figures describe a journal this process no longer owns.
      assertOwnedAccounting?.();
      const state = scopes.get(scopeKey(scope, aliasFor(scope, scopeId)));
      if (!state) return undefined;
      return {
        settled: state.settled,
        reserved: state.reserved,
        unresolved: state.unresolved,
        exhausted: isExhausted(scope, state),
      };
    },

    exhausted(scope: SpendScope, scopeId: string): boolean {
      assertOwnedAccounting?.();
      const state = scopes.get(scopeKey(scope, aliasFor(scope, scopeId)));
      return state !== undefined && isExhausted(scope, state);
    },

    prune(at: number = now()): void {
      assertOwnedAccounting?.();
      // Removal requires BOTH inactive and not exhausted inside the window. An
      // exhausted-but-idle scope that was dropped would be recreated fresh under the
      // same id -- the exact laundering the ceiling exists to stop.
      evictSends(at, false);
      evictScopes(at, false);
    },

    reconfigure(next: SpendReservationPolicy): void {
      assertOwnedAccounting?.();
      policy = next;
    },
  };
}

/**
 * The operator policy in effect. Held beside the singleton ledger rather than inside it
 * because the ledger is built lazily: a configured ceiling has to be remembered from
 * startup until the first request that actually reserves, and an install that configures
 * nothing must still open no journal. The value lives here -- not in
 * spend-reservation-ledger.ts -- so the Worker's admission check answers the same question
 * the container's does from the same defaults.
 */
let sharedPolicy: SpendReservationPolicy = DEFAULT_SPEND_RESERVATION_POLICY;

/** Whether any scope carries a ceiling -- that is, whether anything at all can be refused. */
export function spendCeilingsConfigured(policy: SpendReservationPolicy = sharedPolicy): boolean {
  return policy.root.maxTokens !== undefined
    || policy.identity.maxTokens !== undefined
    || policy.pool.maxTokens !== undefined;
}

/** The policy the process-wide ledger enforces right now. */
export function sharedSpendPolicy(): SpendReservationPolicy {
  return sharedPolicy;
}

/** spend-reservation-ledger.ts's configure/reset seam; not part of the ledger contract. */
export function setSharedSpendPolicy(next: SpendReservationPolicy): void {
  sharedPolicy = next;
}

const spendScopeLimitFromConfig = (scope: OcxSpendScopeConfig | undefined): SpendScopeLimit =>
  scope?.maxTokens !== undefined && Number.isFinite(scope.maxTokens) && scope.maxTokens > 0
    ? { maxTokens: Math.trunc(scope.maxTokens) }
    : {};

/**
 * The ledger policy an operator's `spend` section asks for.
 *
 * An absent section, an empty one, and one whose every ceiling is absent all produce the
 * unconfigured default: observe-only accounting that refuses nothing. That equivalence is the
 * load-bearing part. This ledger is on and journalling by default, so shipping a default
 * ceiling would start refusing real traffic on the first upgrade that ran this code, against
 * a number nobody chose. There is deliberately no default figure here at all.
 */
export function spendPolicyFromConfig(spend: OcxSpendConfig | undefined): SpendReservationPolicy {
  const retentionDays = spend?.retentionDays;
  return {
    root: spendScopeLimitFromConfig(spend?.root),
    identity: spendScopeLimitFromConfig(spend?.identity),
    pool: spendScopeLimitFromConfig(spend?.pool),
    retentionMs: retentionDays !== undefined && Number.isFinite(retentionDays) && retentionDays > 0
      ? Math.trunc(retentionDays) * 24 * 60 * 60_000
      : DEFAULT_SPEND_RESERVATION_POLICY.retentionMs,
  };
}

/**
 * What a token-ceiling refusal has to be able to say.
 *
 * "Budget exhausted" on its own is the failure this repository keeps re-learning: a policy
 * rejection wearing another error's clothing sends an operator to look at the provider. The
 * scope says WHICH ceiling fired -- one task, one account, or the whole pool -- and the limit
 * is the number they would otherwise have to read the journal to recover. The scope ID is
 * deliberately not here: root ids are client thread headers and identity ids are credentials,
 * and the ledger's rule is that neither is written down in the clear.
 */
export interface SpendDenialDetail {
  readonly scope: SpendScope;
  readonly limit: number;
  /** Tokens the refused reservation would have taken the scope to, where that is known. */
  readonly projected?: number;
}

/** Operator-facing name for each scope. What an operator calls it, not what the type calls it. */
export const SPEND_SCOPE_LABEL: Record<SpendScope, string> = {
  root: "task",
  identity: "account",
  pool: "provider pool",
};

/**
 * Thousands separators, done here rather than by `toLocaleString`.
 *
 * A ceiling is an eight- or nine-digit number and an unseparated one is genuinely hard to read
 * against the figure beside it. `toLocaleString` would do this too, but its output depends on
 * the ICU data the runtime happens to carry, and a message a test pins must not differ between
 * a developer's machine and a CI image.
 */
export const formatSpendTokenCount = (tokens: number): string =>
  Math.trunc(tokens).toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");


/**
 * The header a locally refused answer carries the machine-readable code in
 * (workflow-budget.ts). Here rather than there because the Cloudflare Worker answers the
 * same refusal and cannot import the budget module. Nothing upstream sets it, so its
 * presence is conclusive, and the Worker's own retry plumbing finds its refusals by it.
 */
export const WORKFLOW_LOCAL_REFUSAL_HEADER = "x-opencodex-local-refusal";

export type SpendRefusalReason = "workflow-spend-exhausted" | "workflow-spend-undurable";

/**
 * The sentence and code a spend-ledger refusal reports with, for both denial kinds that
 * need it. Lives beside the ledger rather than in workflow-budget.ts so a caller that holds
 * only the SpendReservationLedger types -- the Cloudflare Worker path, which cannot import
 * the budget module -- answers the same refusal with the same words.
 */
export function workflowSpendDenialSummary(
  reason: SpendRefusalReason,
  spend?: SpendDenialDetail,
): { code: string; message: string } {
  switch (reason) {
    case "workflow-spend-exhausted":
      return {
        code: "workflow_spend_exhausted",
        // With the denial in hand the sentence names the ceiling that fired and its number,
        // because the alternative is an operator who can see that something refused and has
        // no way to find out what. Without one -- a caller that knows only the reason -- the
        // original sentence is kept unchanged.
        message: spend
          ? "This proxy refused the request locally: the configured " + SPEND_SCOPE_LABEL[spend.scope]
            + " token ceiling of " + formatSpendTokenCount(spend.limit) + " is spent"
            + (spend.projected !== undefined
              ? " (this send would have taken it to " + formatSpendTokenCount(spend.projected) + ")"
              : "")
            + ", so no provider was contacted. Spend is durable, so it does not roll forward"
            + " with the send window: raise or remove spend." + spend.scope
            + ".maxTokens in config.json to grant more."
          : "This proxy refused the request locally: the task reached a configured token"
            + " ceiling, so no provider was contacted.",
      };
    case "workflow-spend-undurable":
      return {
        code: "workflow_spend_undurable",
        message: "This proxy refused the request locally: the token reservation could not be made"
          + " durable and a configured ceiling requires it, so no provider was contacted.",
      };
  }
}

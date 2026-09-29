// The spend journal and reservation ledger for a Cloudflare deployment, owned by the hub
// Durable Object (src/lib/spend-reservation-core.ts has the accounting). The file journal's
// single-writer guarantee (spend-reservation-ledger.ts) becomes the DO's single-activity
// guarantee here: SQLite rows are the journal, ctx.storage.sql is synchronous, and every
// mutation this object makes is serial by construction.
//
// Two writers reach it. The Worker reserves and settles over RPC during a native turn, and
// the container's replica journal (src/lib/durable-spend-ledger.ts) pushes each record it
// books through appendJournalRecord — already journaled there, so importRecord moves the
// accounting without writing the row a second time. An append that fails there stays
// unresolved in the replica, which is the fail-closed direction.
//
// Kept free of Workers-only imports (the DurableObjectState typing is structural) so
// tests/service/cloudflare-spend-ledger.test.ts can drive it.
import {
  createSpendReservationLedger,
  parseSpendJournalRecord,
  spendPolicyFromConfig,
  DEFAULT_SPEND_RESERVATION_POLICY,
  type ScopeSpendSnapshot,
  type SpendJournal,
  type SpendReservationDecision,
  type SpendReservationLedger,
  type SpendReservationRequest,
  type SpendScope,
  type SpendUsage,
} from "../../../src/lib/spend-reservation-core";

/** The storage surface ctx.storage.sql provides; structural so tests can fake it. */
export interface SpendLedgerSqlStorage {
  exec<T = Record<string, unknown>>(
    query: string,
    ...bindings: unknown[]
  ): { toArray(): T[] };
  transactionSync<T>(fn: () => T): T;
}

const JOURNAL_TABLE = "ocx_spend_journal";
const META_TABLE = "ocx_spend_meta";
// The alias salt must be stable for the journal's life: replay matches a live request to its
// recorded spend through aliases, and a new salt would hand every scope a fresh allowance.
const SALT_KEY = "spend_salt";

/** 256 bits of salt as hex; a copyable journal still cannot be reversed by dictionary. */
function mintSpendSalt(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(32));
  return [...bytes].map(byte => byte.toString(16).padStart(2, "0")).join("");
}

/** The SpendJournal over DO SQLite. Appends dedupe on the line: a replica that retries a row
 * whose ack was lost must not journal it twice. */
export class DurableSpendJournal implements SpendJournal {
  constructor(
    private readonly sql: SpendLedgerSqlStorage,
    readonly salt: string,
  ) {
    this.sql.exec(
      `CREATE TABLE IF NOT EXISTS ${JOURNAL_TABLE} (seq INTEGER PRIMARY KEY AUTOINCREMENT, line TEXT NOT NULL)`,
    );
    this.sql.exec(
      `CREATE TABLE IF NOT EXISTS ${META_TABLE} (key TEXT PRIMARY KEY, value TEXT NOT NULL)`,
    );
  }

  /** Journal lines in the order they were written — replay's input. */
  read(): string[] {
    return this.sql
      .exec<{ line: string }>(`SELECT line FROM ${JOURNAL_TABLE} ORDER BY seq`)
      .toArray()
      .map(row => row.line);
  }

  append(line: string): void {
    this.sql.exec(
      `INSERT INTO ${JOURNAL_TABLE} (line) SELECT ? WHERE NOT EXISTS (SELECT 1 FROM ${JOURNAL_TABLE} WHERE line = ?)`,
      line,
      line,
    );
  }

  rewrite(lines: string[]): void {
    // Compaction is atomic: a crash inside it leaves the previous journal, never a torn one.
    this.sql.transactionSync(() => {
      this.sql.exec(`DELETE FROM ${JOURNAL_TABLE}`);
      for (const line of lines) {
        this.sql.exec(`INSERT INTO ${JOURNAL_TABLE} (line) VALUES (?)`, line);
      }
    });
  }
}

/** The salt this deployment's journal was minted with, creating it on first use. */
export function spendLedgerSalt(sql: SpendLedgerSqlStorage): string {
  sql.exec(`CREATE TABLE IF NOT EXISTS ${META_TABLE} (key TEXT PRIMARY KEY, value TEXT NOT NULL)`);
  const stored = sql
    .exec<{ value: string }>(`SELECT value FROM ${META_TABLE} WHERE key = ?`, SALT_KEY)
    .toArray()[0]?.value;
  if (typeof stored === "string" && /^[0-9a-f]{64}$/.test(stored)) return stored;
  const minted = mintSpendSalt();
  sql.exec(`INSERT INTO ${META_TABLE} (key, value) VALUES (?, ?)`, SALT_KEY, minted);
  return minted;
}

const isCount = (value: unknown): value is number =>
  typeof value === "number" && Number.isFinite(value) && value >= 0;
const isSendId = (value: unknown): value is string => typeof value === "string" && value.length > 0;

/** Structural check for an RPC-carried reservation request. The ledger's own field validation
 * runs on journal replay; an RPC arg that is not request-shaped is rejected before it can
 * write a malformed row. */
export function isSpendReservationRequest(value: unknown): value is SpendReservationRequest {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const request = value as SpendReservationRequest;
  if (!isSendId(request.sendId) || !request.scopes || typeof request.scopes !== "object") return false;
  const scopes = request.scopes as Record<string, unknown>;
  for (const key of ["rootId", "identityId", "poolId"] as const) {
    if (scopes[key] !== undefined && typeof scopes[key] !== "string") return false;
  }
  return isCount(request.inputTokens) && isCount(request.outputCeilingTokens)
    && (request.alreadySent === undefined || request.alreadySent === true)
    && (request.at === undefined || isCount(request.at));
}

export function isSpendUsage(value: unknown): value is SpendUsage {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const usage = value as SpendUsage;
  return isCount(usage.inputTokens) && isCount(usage.outputTokens);
}

/**
 * The deployment's reservation authority. One per hub activation; the journal behind it is
 * durable in the object's SQLite, so a hibernated or migrated object replays the same books.
 * Policy is refreshed from the hub's stored config on every call — a config the container
 * commits mid-turn changes what is refused from the next call, exactly as a reload does for
 * configureSharedSpendLedger.
 */
export class DurableSpendLedger {
  private ledger: SpendReservationLedger | undefined;
  private journal: DurableSpendJournal | undefined;

  constructor(
    private readonly sql: SpendLedgerSqlStorage,
    /** The config.json text the Worker routes with (nativeConfigSource), per call. */
    private readonly configText: () => Promise<string | undefined>,
  ) {}

  /** The journal object shared by the ledger and the replica routes. */
  private spendJournal(): DurableSpendJournal {
    this.journal ??= new DurableSpendJournal(this.sql, spendLedgerSalt(this.sql));
    return this.journal;
  }

  /** Constructed lazily: replay costs a full scan, and a hub that never reserves never pays it. */
  private spend(): SpendReservationLedger {
    if (!this.ledger) {
      const journal = this.spendJournal();
      this.ledger = createSpendReservationLedger({
        journal,
        salt: journal.salt,
        // Replaced on every call by reconfigure below; the default only covers the window
        // before the first refresh.
        policy: DEFAULT_SPEND_RESERVATION_POLICY,
        // No assertOwnedAccounting: ownership here is being the single-threaded Durable
        // Object itself, not holding a filesystem lease.
      });
    }
    return this.ledger;
  }

  /** The ledger with its policy refreshed from the stored config's `spend` section. */
  private async ready(): Promise<SpendReservationLedger> {
    const ledger = this.spend();
    const text = await this.configText();
    let policy = DEFAULT_SPEND_RESERVATION_POLICY;
    if (text) {
      try {
        const config = JSON.parse(text) as { spend?: Parameters<typeof spendPolicyFromConfig>[0] };
        policy = spendPolicyFromConfig(config?.spend);
      } catch {
        // An unparseable config declines turns before this is reached; refuse nothing extra.
      }
    }
    ledger.reconfigure(policy);
    return ledger;
  }

  async spendReserve(request: SpendReservationRequest): Promise<SpendReservationDecision> {
    if (!isSpendReservationRequest(request)) throw new TypeError("spend reservation request required");
    return (await this.ready()).reserve(request);
  }
  async spendAbandon(sendId: string): Promise<boolean> {
    if (!isSendId(sendId)) return false;
    return (await this.ready()).abandon(sendId);
  }
  async spendSettle(sendId: string, usage: SpendUsage): Promise<boolean> {
    if (!isSendId(sendId) || !isSpendUsage(usage)) return false;
    return (await this.ready()).settle(sendId, usage);
  }
  async spendMarkLost(sendId: string): Promise<boolean> {
    if (!isSendId(sendId)) return false;
    return (await this.ready()).markLost(sendId);
  }
  async spendMarkDispatched(sendId: string): Promise<boolean> {
    if (!isSendId(sendId)) return false;
    return (await this.ready()).markDispatched(sendId);
  }
  async spendKnows(sendId: string): Promise<boolean> {
    if (!isSendId(sendId)) return false;
    return (await this.ready()).knows(sendId);
  }
  async spendSnapshot(scope: SpendScope, scopeId: string): Promise<ScopeSpendSnapshot | undefined> {
    if (scope !== "root" && scope !== "identity" && scope !== "pool") return undefined;
    if (typeof scopeId !== "string" || scopeId.length === 0) return undefined;
    return (await this.ready()).snapshot(scope, scopeId);
  }

  /**
   * The journal as the container's replica journal reads it: the salt first, then every row
   * with its sequence so the replica's resync can tell new rows from the ones it pushed.
   * Lease-gated by the caller (state-routes.ts), like the documents it travels beside.
   */
  journalSnapshot(): { salt: string; entries: { seq: number; line: string }[] } {
    const journal = this.spendJournal();
    const entries = this.sql
      .exec<{ seq: number; line: string }>(`SELECT seq, line FROM ${JOURNAL_TABLE} ORDER BY seq`)
      .toArray();
    return { salt: journal.salt, entries };
  }

  /**
   * One record the container's replica journal already wrote locally and is now reporting.
   * The journal append dedupes a retry by content, so the returned sequence is the row's
   * whether or not this call wrote it; importRecord keeps the live books in step — the
   * container's in-flight reservation is what makes a Worker turn's reserve see it.
   * Returns null for a line that is not a journal record, so the route can say 400.
   */
  appendJournalRecord(line: string): number | null {
    const record = parseSpendJournalRecord(line);
    if (!record) return null;
    // Journal first, books second: a record the ledger imports but the journal lost would be
    // invisible to the next activation's replay.
    const journal = this.spendJournal();
    journal.append(line);
    const seq = this.sql
      .exec<{ seq: number }>(`SELECT seq FROM ${JOURNAL_TABLE} WHERE line = ? ORDER BY seq DESC LIMIT 1`, line)
      .toArray()[0]?.seq;
    if (seq === undefined) return null;
    // Idempotent by construction: replay's applyRecord no-ops on a send id it already knows,
    // so a retried push moves nothing the first push already moved.
    this.spend().importRecord(record);
    return seq;
  }
}

// The container's view of the spend journal on a Cloudflare deployment, where the hub's
// Durable Object is the reservation authority (deploy/cloudflare/src/spend-ledger.ts).
// sharedSpendLedger() in spend-reservation-ledger.ts builds createSpendReservationLedger
// over this SpendJournal instead of the file journal whenever durableMirrorEnabled().
//
// The ledger's contract is synchronous, so this journal is a write-through replica:
// `read()` returns the journal as this process last saw the DO plus every line it appended
// itself, and `append()` books the line locally at once -- which is what lets the same sync
// call sites keep their semantics -- then pushes the row to the DO on a serial queue. A push
// that fails CLOSES the journal: later appends throw, so a reservation the DO will never
// learn about is refused under a configured ceiling rather than diverging the books silently.
//
// `resync()` fetches the DO's rows ahead of the last sequence this process confirmed, so a
// send the Worker reserved between this replica's calls still reaches the books. The DO
// dedupes a row by content, so a push retried after its answer was lost is idempotent.
//
// Two documented divergences, both fail-safe:
//  * A replica that lost a record keeps accounting it in memory while the DO never saw it;
//    the books the Worker reserves against are the DO's, which are complete.
//  * A DO-side journal compaction (rewrite) makes sequence numbers incomparable mid-flight.
//    resync reports `reset`, and the caller rebuilds its ledger rather than import a
//    checkpoint over divergent maps.
import { stateRequest } from "./durable-mirror";
import { sleepWithAbort } from "./upstream-retry";
import { parseSpendJournalRecord, type SpendJournal, type SpendJournalRecord } from "./spend-reservation-core";

/**
 * Every failure of this journal -- unreadable bootstrap, a refused or lost append, a lease
 * that moved -- is the same answer to the ledger: this replica cannot prove the DO knows
 * what it knows. It replaces the file journal's SpendLedgerOwnerError, which implies a
 * filesystem ownership no Cloudflare journal has.
 */
export class DurableSpendJournalError extends Error {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "DurableSpendJournalError";
  }
}

export interface SpendJournalEntry {
  readonly seq: number;
  readonly line: string;
}

/** The two calls the container's replica needs, over the same channel documents travel. */
export interface SpendJournalTransport {
  /** The DO's journal rows plus the salt it aliases them with, or null when unreachable. */
  read(): Promise<{ salt: string; entries: SpendJournalEntry[] } | null>;
  /** Append one line to the DO's journal; resolves to its sequence, or null on failure. */
  append(line: string): Promise<number | null>;
}

const stateTransport: SpendJournalTransport = {
  async read() {
    const response = await stateRequest("/spend-journal");
    if (!response?.ok) return null;
    const body = await response.json().catch(() => undefined) as { salt?: unknown; entries?: unknown } | undefined;
    if (typeof body?.salt !== "string" || !Array.isArray(body.entries)) return null;
    const entries: SpendJournalEntry[] = [];
    for (const raw of body.entries) {
      if (!raw || typeof raw !== "object") return null;
      const entry = raw as { seq?: unknown; line?: unknown };
      if (typeof entry.seq !== "number" || !Number.isSafeInteger(entry.seq) || typeof entry.line !== "string") return null;
      entries.push({ seq: entry.seq, line: entry.line });
    }
    return { salt: body.salt, entries };
  },
  async append(line) {
    const response = await stateRequest("/spend-journal", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ line }),
    });
    if (!response?.ok) return null;
    const body = await response.json().catch(() => undefined) as { seq?: unknown } | undefined;
    return typeof body?.seq === "number" && Number.isSafeInteger(body.seq) ? body.seq : null;
  },
};

let transport: SpendJournalTransport = stateTransport;

/** Test seam: point the replica journal at an in-memory DO. */
export function setDurableSpendJournalTransportForTests(next: SpendJournalTransport | null): void {
  transport = next ?? stateTransport;
}

/** What resync found, for the caller that owns the ledger built over this journal. */
export type SpendJournalResync =
  | { kind: "unchanged" }
  /** Journal lines the DO holds that this replica has not applied yet, in write order. */
  | { kind: "added"; lines: string[] }
  /** The DO's journal was compacted: sequence comparison is void, rebuild from scratch. */
  | { kind: "reset" };

/**
 * The DO-backed replica journal for this process, or a construction error.
 *
 * `read()` throws until the bootstrap prefetch lands, which is what makes `sharedSpendLedger()`
 * fail closed: callers get DurableSpendJournalError until the journal's contents are known,
 * never a ledger replayed from an empty view of real spend.
 */
export class DurableSpendJournal implements SpendJournal {
  /** The replica's view: remote lines in sequence order, then rows this process appended. */
  private lines: string[] | undefined;
  private saltValue: string | undefined;
  private loading: Promise<void> | undefined;
  private failed = false;
  private queue: Promise<void> = Promise.resolve();
  /** Every line read() has ever reported, so a resync imports each remote row exactly once. */
  private readonly known = new Set<string>();
  /** The highest remote sequence merged into `lines`; entries past it are new. */
  private watermark = 0;
  /** Lines this process pushed but has not yet seen confirmed by the DO, in arrival order. */
  private unconfirmed: string[] = [];

  /** The alias salt the DO minted; records must be comparable to the journal they live in. */
  get salt(): string {
    if (this.saltValue === undefined) {
      void this.load();
      throw new DurableSpendJournalError("the durable spend journal has not been read yet");
    }
    return this.saltValue;
  }

  /**
   * Pull the DO's current journal. Safe to call repeatedly; the first success stands because
   * the replica's own appends are already folded in.
   */
  prefetch(): Promise<void> {
    return this.load();
  }

  private fold(entries: SpendJournalEntry[], salt: string): void {
    // Synchronous, so an append cannot interleave between the diff and the commit.
    const remoteLines = entries.map(entry => entry.line);
    const confirmed = new Set(remoteLines);
    this.unconfirmed = this.unconfirmed.filter(line => !confirmed.has(line));
    for (const line of this.unconfirmed) this.known.add(line);
    this.saltValue = salt;
    this.lines = [...remoteLines, ...this.unconfirmed];
    for (const line of this.lines) this.known.add(line);
    this.watermark = entries.reduce((max, entry) => Math.max(max, entry.seq), 0);
  }

  private load(): Promise<void> {
    if (this.lines !== undefined) return Promise.resolve();
    this.loading ??= transport.read()
      .then(snapshot => {
        if (snapshot === null) {
          throw new DurableSpendJournalError("the Durable Object spend journal could not be read");
        }
        this.fold(snapshot.entries, snapshot.salt);
      })
      .then(() => undefined)
      .catch(error => {
        this.loading = undefined;
        throw error;
      });
    return this.loading;
  }

  read(): string[] {
    if (this.lines === undefined) {
      void this.load();
      throw new DurableSpendJournalError("the durable spend journal has not been read yet");
    }
    return [...this.lines];
  }

  append(line: string): void {
    if (this.failed) {
      // A row already failed to reach the DO: admitting another would deepen a divergence
      // neither side can reconcile, the same way a file journal that lost a write refuses.
      throw new DurableSpendJournalError("an earlier spend-journal record never reached the Durable Object");
    }
    if (this.lines !== undefined) {
      this.lines.push(line);
      this.known.add(line);
    }
    this.unconfirmed.push(line);
    const push = async (): Promise<void> => {
      // One retry, because the transport is a single fetch; a second try separates a blip
      // from a dead DO without holding the queue open indefinitely.
      for (let attempt = 0; attempt < 2; attempt += 1) {
        try {
          const seq = await transport.append(line);
          if (seq !== null) {
            this.watermark = Math.max(this.watermark, seq);
            return;
          }
        } catch { /* fall through to the retry or the failure below */ }
        if (attempt === 0) await sleepWithAbort(500);
      }
      throw new DurableSpendJournalError("a spend-journal record could not reach the Durable Object");
    };
    this.queue = this.queue.then(push).catch(() => { this.failed = true; });
    this.queue.finally(() => {
      const at = this.unconfirmed.indexOf(line);
      if (at >= 0 && !this.failed) this.unconfirmed.splice(at, 1);
    });
  }

  /**
   * Journal rows the DO holds ahead of this replica's view, parsed and ready for
   * SpendReservationLedger.importRecord. Cheap when nothing moved.
   */
  async resync(): Promise<SpendJournalResync> {
    if (this.lines === undefined) await this.load();
    const snapshot = await transport.read();
    if (snapshot === null) throw new DurableSpendJournalError("the Durable Object spend journal could not be read");
    const entries = snapshot.entries;
    // A compaction replaces the journal with one checkpoint row under a fresh AUTOINCREMENT
    // sequence, so no seq relation distinguishes it: the tell is content — a rewritten
    // journal shares not one line with the rows this replica already merged. Folding it as a
    // delta would apply a checkpoint (which resets scope totals outright) over divergent
    // maps; reset wins instead.
    const compacted = (this.lines?.length ?? 0) > 0 && entries.every(entry => !this.known.has(entry.line));
    if (compacted || snapshot.salt !== this.saltValue) {
      this.fold(entries, snapshot.salt);
      return { kind: "reset" };
    }
    const added: string[] = [];
    const confirmed = new Set(entries.map(entry => entry.line));
    this.unconfirmed = this.unconfirmed.filter(line => !confirmed.has(line));
    for (const entry of entries) {
      if (entry.seq <= this.watermark || this.known.has(entry.line)) continue;
      added.push(entry.line);
      this.known.add(entry.line);
      this.lines!.push(entry.line);
      this.watermark = entry.seq;
    }
    if (added.length === 0) return { kind: "unchanged" };
    return { kind: "added", lines: added };
  }

  /** Parse the rows resync reported; the caller imports each into its ledger. */
  static parseResynced(lines: string[]): SpendJournalRecord[] {
    const records: SpendJournalRecord[] = [];
    for (const line of lines) {
      const record = parseSpendJournalRecord(line);
      if (record) records.push(record);
    }
    return records;
  }
}

let sharedJournal: DurableSpendJournal | undefined;

/**
 * The process-wide replica journal. It is per-PROCESS rather than per-home like the file
 * journal because the DO's journal is per-DEPLOYMENT: there is exactly one on a Cloudflare
 * install, and the boot id that names this writer comes from the environment.
 */
export function durableSpendJournal(): DurableSpendJournal {
  sharedJournal ??= new DurableSpendJournal();
  return sharedJournal;
}

/**
 * Begin loading the DO's journal so the first reserving call does not have to wait for a
 * fetch it could have started at boot. Start is the only safe place to call this: earlier
 * reads race container initialization the same way document mirroring does.
 */
export function prefetchDurableSpendJournal(): void {
  void durableSpendJournal().prefetch().catch(error => {
    console.warn(`[spend] Durable Object journal not readable yet: ${error instanceof Error ? error.message : String(error)}`);
  });
}

/** Test seam; the replica is process-wide, so reset it with the ledger built over it. */
export function resetDurableSpendJournalForTest(): void {
  sharedJournal = undefined;
}

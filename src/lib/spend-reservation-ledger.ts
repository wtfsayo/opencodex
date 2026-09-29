/**
 * Durable token spend reservation, above the send-count workflow guard (#4546).
 *
 * The count cap treats a 1k-token send and a 150k-token send as the same unit, and the
 * in-memory ledger forgets everything on restart: an exhausted root came back with a fresh
 * allowance after every relaunch, and a second process never saw the first one's spend at
 * all. This ledger reserves TOKENS before dispatch and rebuilds its state from a journal
 * under the opencodex home directory, so an exhausted scope is still exhausted after a
 * restart.
 *
 * A reservation is always the request's whole input plus its ENFORCEABLE output ceiling --
 * the caller's max_output_tokens, or the model's documented cap when the caller sent none.
 * Never an optimistic estimate, and never shrunk by a cache-hit expectation: a prefix that
 * misses is billed in full, so the safety figure reserves as if it misses. Cache
 * expectations may inform efficiency reporting; they do not move this number.
 *
 * Admission requires, at every scope that applies at once -- root workflow, authenticated
 * identity, and account pool:
 *
 *   settled spend + in-flight reservations + unresolved spend + this reservation <= limit
 *
 * Unresolved spend is the conservative residue of a send whose usage frame was lost: the
 * tokens may have been billed, so the reservation is moved to unresolved rather than
 * released. Minting a new root id mints no new budget because the identity and pool scopes
 * still hold the spend.
 *
 * SUPPORTED TOPOLOGY: one live writer owns the journal. Every server and direct
 * shared-ledger caller must hold the state-directory SQLite lease before replay, append or
 * compaction. On a Cloudflare deployment (durable-mirror.ts's boot id) the journal lives in
 * the hub Durable Object instead: the DO is the single writer for the reservation authority,
 * the Worker's native paths reserve through its RPC surface, and this process's callers see
 * the write-through replica in durable-spend-ledger.ts, which refuses under a configured
 * ceiling the moment a journal row cannot reach the DO. Independent homes remain
 * independent; multi-host shared storage still needs a distributed transaction boundary and
 * is outside this local lease.
 *
 * Five properties this ledger owes its callers. Each one was absent in the first draft, and a
 * budget that can be bypassed is worse than no budget because it looks like protection:
 *
 * 1. IDENTITY OF A SEND. A send id is either KNOWN -- and then reserving it again is refused
 *    rather than waved through booking nothing -- or FULLY forgotten, and then it books a
 *    fresh reservation. There is no third state where the ledger recognises an id and
 *    charges nothing for it, which is what let one id authorise unlimited physical sends.
 * 2. DURABILITY BEFORE ADMISSION. Under a configured limit the reserve record must be on
 *    disk before the request is admitted. Failing open on a disk-full or permission error
 *    forgets the request across a restart, which is the exact case durability exists for.
 *    Observe-only mode still admits, and says so through `durable: false`.
 * 3. REPLAY VALIDATES. Every journal record is checked field by field before it moves a
 *    counter. A corrupt record in the MIDDLE of the file would silently undercount, so it
 *    fails accounting closed instead; only an unparseable FINAL line -- a torn tail write --
 *    is dropped quietly.
 * 4. BOUNDED RETENTION. Cleanup runs automatically, writes durable tombstones so replay
 *    cannot resurrect what it removed, and compacts the journal to a checkpoint. When
 *    nothing can be evicted safely, admission is refused rather than made room for by
 *    forgetting an exhausted scope -- which is the laundering this layer prevents.
 * 5. NOTHING IDENTIFYING ON DISK. Root ids come from a client header and identity ids are
 *    credential ids, so the journal stores salted aliases only, under owner-only permissions
 *    that are re-applied to an EXISTING file rather than trusted from its creation.
 *
 * The ledger itself -- types, journal contract, replay and accounting -- lives in
 * spend-reservation-core.ts, which the Cloudflare Worker bundle can also import. This file
 * keeps the file-system journal, the ownership proof and the process-wide singleton.
 */

import { appendFileSync, chmodSync, closeSync, lstatSync, mkdirSync, openSync, readFileSync, renameSync, statSync, unlinkSync, writeFileSync } from "node:fs";
import { randomBytes } from "node:crypto";
import { dirname } from "node:path";
// Definition-site import, not the ../config barrel -- same reasoning as
// src/quota/reset-seen-store.ts: the barrel pulls ~154 modules into a hot path.
import { getConfigDir } from "../config/paths";
import { durableMirrorEnabled } from "./durable-mirror";
import { durableSpendJournal, DurableSpendJournal, DurableSpendJournalError } from "./durable-spend-ledger";
import { assertNotRealHomeUnderTest } from "./test-home-guard";
// Windows chmod does not remove inherited ACEs; this is the repository's icacls path.
import { hardenSecretPath } from "./windows-secret-acl";
import { assertSpendLedgerOwnerHeld, assertStorageOwned, bindSpendLedgerOwnerHome, mintSpendLedgerStorage, onSpendLedgerOwnerReleased, resetSpendLedgerOwnerBindingForTest, spendLedgerOwnerSnapshot, spendLedgerStoragePath, type SpendLedgerStorage } from "./spend-ledger-owner";
import {
  createSpendReservationLedger,
  setSharedSpendPolicy,
  sharedSpendPolicy,
  SpendLedgerOwnerError,
  spendCeilingsConfigured,
  DEFAULT_SPEND_RESERVATION_POLICY,
} from "./spend-reservation-core";
import type {
  ScopeSpendSnapshot,
  SpendJournal,
  SpendReservationLedger,
  SpendReservationPolicy,
} from "./spend-reservation-core";

export {
  createSpendReservationLedger,
  DEFAULT_SPEND_RESERVATION_POLICY,
  formatSpendTokenCount,
  WORKFLOW_LOCAL_REFUSAL_HEADER,
  parseSpendJournalRecord,
  sharedSpendPolicy,
  SPEND_SCOPE_LABEL,
  spendCeilingsConfigured,
  SpendLedgerOwnerError,
  spendPolicyFromConfig,
  workflowSpendDenialSummary,
} from "./spend-reservation-core";
export type {
  ScopeSpendSnapshot,
  SpendDenial,
  SpendDenialDetail,
  SpendJournal,
  SpendJournalRecord,
  SpendLedgerOwnerErrorCode,
  SpendRefusalReason,
  SpendReservationDecision,
  SpendReservationLedger,
  SpendReservationPolicy,
  SpendReservationRequest,
  SpendScope,
  SpendScopeLimit,
  SpendScopes,
  SpendUsage,
} from "./spend-reservation-core";

// The singleton belongs to the state directory it was built for. Releasing ownership hands that
// directory to whoever comes next, so the in-memory copy goes with it and the next construction
// replays the journal.
onSpendLedgerOwnerReleased(() => { sharedLedger = undefined; });

export const SPEND_LEDGER_JOURNAL_FILENAME = "spend-ledger.jsonl";
/**
 * Per-install alias salt, beside the journal. Losing it is exactly as bad as losing the
 * journal -- both reset accounting, both live in the same 0700 directory -- so it is not a
 * new weakness, and keeping it out of the journal stops a copied or attached journal from
 * being reversible by dictionary attack on guessable pool and identity ids.
 */
export const SPEND_LEDGER_SALT_FILENAME = "spend-ledger.salt";

/**
 * Re-apply owner-only permissions to a file that already exists.
 *
 * `mode` in a write option is honoured only when the file is CREATED, so a journal that was
 * created loose -- by an older build, a restored backup, or a lax umask -- would keep its
 * mode forever. Best-effort by design: a non-owner cannot chmod, and failing every append
 * over it would be worse than the loose mode it is fixing.
 *
 * `force` marks the points where the WINDOWS ACL can actually be wrong: creation, compaction,
 * and each process's replay. Windows chmod cannot drop inherited ACEs, so icacls is the real
 * boundary there, and its memo keys on the file's ctime -- which every append changes. Running
 * it per reservation would therefore spawn a process per send while protecting nothing an
 * append can alter. On POSIX the mode is checked on every write and repaired the moment it
 * drifts, which costs one stat.
 */
function hardenLedgerFile(path: string, options: { readonly force?: boolean } = {}): void {
  if (process.platform === "win32") {
    if (options.force) hardenSecretPath(path, { required: false });
    return;
  }
  try {
    if ((statSync(path).mode & 0o777) === 0o600) return;
    chmodSync(path, 0o600);
  } catch { /* best-effort: a non-owner cannot chmod */ }
}

/**
 * Fault injection for the journal's own filesystem steps. Internal test contract, not config.
 *
 * The compaction cleanup only runs when a step after the exclusive create fails, and there is no
 * portable way to make a validate, harden or rename fail on demand. Without a seam the cleanup
 * would ship asserted by reading alone, which is how a failure path stays broken.
 */
export type SpendJournalFaultStep = "stat" | "create" | "write" | "validate" | "harden" | "rename";

let journalFaultForTests: ((step: SpendJournalFaultStep, temp: string) => void) | undefined;

export function setSpendJournalFaultForTests(
  fault: ((step: SpendJournalFaultStep, temp: string) => void) | undefined,
): void {
  journalFaultForTests = fault;
}

/**
 * Does a directory entry exist here, whatever it points at?
 *
 * `existsSync` follows the link, so a symlink whose target is absent reads as "no file" and an
 * append then creates that target somewhere else entirely. The entry itself is what decides
 * whether the safety check runs.
 */
function ledgerEntryExists(path: string): boolean {
  try {
    journalFaultForTests?.("stat", path);
    lstatSync(path);
    return true;
  } catch (error) {
    // Only a genuinely absent entry is absent. Treating every failure as "no file" meant a
    // permission denial or an I/O error skipped assertSafeLedgerFile entirely and let the
    // append proceed against whatever is actually there, which is the case that check exists
    // for. An entry we cannot inspect is a refusal, not an empty slot.
    //
    // Raised as the module's typed error rather than the raw fs error: the path and errno of a
    // state file are not something a client should be handed.
    if ((error as NodeJS.ErrnoException | null)?.code === "ENOENT") return false;
    throw new SpendLedgerOwnerError(
      "SPEND_LEDGER_OWNER_UNAVAILABLE",
      "Spend-ledger storage could not be inspected safely.",
      { cause: error },
    );
  }
}

function assertSafeLedgerFile(path: string): void {
  const stat = lstatSync(path);
  if (!stat.isFile() || stat.isSymbolicLink() || stat.nlink !== 1
    || (process.platform !== "win32" && stat.uid !== process.getuid!())) {
    throw new SpendLedgerOwnerError(
      "SPEND_LEDGER_OWNER_UNAVAILABLE",
      "Spend-ledger storage could not be opened safely.",
    );
  }
}

/**
 * The production journal. Its location comes from the owned state directory and every touch
 * proves that ownership, so there is no entrypoint here that writes a caller-chosen path.
 */
export function createOwnedFileSpendJournal(storage: SpendLedgerStorage): SpendJournal {
  assertStorageOwned(storage);
  const path = spendLedgerStoragePath(storage);
  const ensureDir = (): string => {
    const dir = dirname(path);
    // The guard runs before any mutation so a rejected write leaves nothing behind.
    assertNotRealHomeUnderTest(dir);
    mkdirSync(dir, { recursive: true, mode: 0o700 });
    return dir;
  };
  return {
    read(): string[] {
      assertStorageOwned(storage);
      if (!ledgerEntryExists(path)) return [];
      assertSafeLedgerFile(path);
      // Replay is once per process and is the moment a journal inherited from an older build
      // or a restored backup first passes through here.
      hardenLedgerFile(path, { force: true });
      return readFileSync(path, "utf8").split("\n").filter((line) => line.length > 0);
    },
    append(line: string): void {
      assertStorageOwned(storage);
      ensureDir();
      const created = !ledgerEntryExists(path);
      if (!created) assertSafeLedgerFile(path);
      appendFileSync(path, line + "\n", { encoding: "utf8", mode: 0o600 });
      assertSafeLedgerFile(path);
      hardenLedgerFile(path, { force: created });
    },
    rewrite(lines: string[]): void {
      assertStorageOwned(storage);
      ensureDir();
      // Same directory, so the rename is atomic on the same filesystem: a crash mid-compaction
      // leaves either the old journal or the new one, never a half-written ledger.
      if (ledgerEntryExists(path)) assertSafeLedgerFile(path);
      const temp = `${path}.compact-${process.pid}-${randomBytes(6).toString("hex")}`;
      // The creation is INSIDE the cleanup, not before it. The name carries random bytes, so a
      // failure anywhere after the entry exists used to leave a uniquely named file and the next
      // attempt made another: repeated failures accumulated instead of overwriting one fixed
      // name. Only an entry this call created is removed, so a name that turned out to belong to
      // something else is left alone, and the original journal and the primary error survive.
      let fd: number | undefined;
      let created = false;
      let renamed = false;
      try {
        // Exclusive create FIRST, so "this entry is ours" is a fact rather than a guess about
        // which error a combined write threw. EEXIST leaves created false and the name is left
        // alone; every failure after this point is cleaned because the entry is provably ours,
        // including a write that stopped partway through.
        journalFaultForTests?.("create", temp);
        fd = openSync(temp, "wx", 0o600);
        created = true;
        journalFaultForTests?.("write", temp);
        writeFileSync(fd, lines.map((line) => line + "\n").join(""), { encoding: "utf8" });
        closeSync(fd);
        fd = undefined;
        journalFaultForTests?.("validate", temp);
        assertSafeLedgerFile(temp);
        journalFaultForTests?.("harden", temp);
        hardenLedgerFile(temp, { force: true });
        journalFaultForTests?.("rename", temp);
        renameSync(temp, path);
        renamed = true;
      } finally {
        if (fd !== undefined) {
          try { closeSync(fd); } catch { /* the compaction failure is the one to report */ }
        }
        if (created && !renamed) {
          try { unlinkSync(temp); } catch { /* same */ }
        }
      }
      assertSafeLedgerFile(path);
      hardenLedgerFile(path, { force: true });
    },
  };
}

/**
 * Load the per-install alias salt, minting it on first use.
 *
 * The salt must be STABLE across restarts or replay cannot match a live request to its own
 * recorded spend, which would hand every scope a fresh allowance -- so it is a file, not a
 * per-process value.
 */
export function loadOrCreateSpendLedgerSalt(storage: SpendLedgerStorage): string {
  assertStorageOwned(storage);
  const path = spendLedgerStoragePath(storage);
  if (ledgerEntryExists(path)) {
    assertSafeLedgerFile(path);
    hardenLedgerFile(path, { force: true });
    const existing = readFileSync(path, "utf8").trim();
    if (/^[0-9a-f]{32,}$/.test(existing)) return existing;
    throw new SpendLedgerOwnerError(
      "SPEND_LEDGER_OWNER_UNAVAILABLE",
      "Spend-ledger storage could not be opened safely.",
    );
  }
  const dir = dirname(path);
  assertStorageOwned(storage);
  assertNotRealHomeUnderTest(dir);
  mkdirSync(dir, { recursive: true, mode: 0o700 });
  const salt = randomBytes(32).toString("hex");
  writeFileSync(path, salt + "\n", { encoding: "utf8", mode: 0o600, flag: "wx" });
  assertSafeLedgerFile(path);
  hardenLedgerFile(path, { force: true });
  return salt;
}

let sharedLedger: SpendReservationLedger | undefined;

/**
 * Apply an operator policy to the process-wide ledger.
 *
 * Startup calls this with the loaded config, and a reload may call it again: the ledger keeps
 * every figure it has already accounted, so changing a ceiling changes what is refused from
 * here on and never what was spent. It does not CREATE the ledger -- an install that
 * configures no ceiling must not open a journal merely because the server started.
 */
export function configureSharedSpendLedger(policy: SpendReservationPolicy): void {
  // Recording a policy value touches no journal, so it needs no ownership. Changing a ledger
  // that already exists does, because that ledger is a live view of an owned directory.
  if (sharedLedger) {
    assertSpendLedgerOwnerHeld();
    bindSpendLedgerOwnerHome();
  }
  setSharedSpendPolicy(policy);
  sharedLedger?.reconfigure(policy);
}

/**
 * Process-wide ledger backed by the journal under OPENCODEX_HOME -- or, on a Cloudflare
 * deployment, by the write-through replica over the hub's Durable Object journal. Created
 * lazily so importing the module -- or running a request path that never reserves -- touches
 * no disk. One directory at a time, not one directory for the life of the process: the
 * singleton is discarded when its ownership ends, so a later directory replays its own
 * journal rather than inheriting figures from the previous one.
 */
export function sharedSpendLedger(): SpendReservationLedger {
  assertSpendLedgerOwnerHeld();
  bindSpendLedgerOwnerHome();
  if (!sharedLedger) {
    if (durableMirrorEnabled()) {
      // The Durable Object holds the journal; this replica decides admission locally over a
      // copy of its lines and reports every record it appends back. A replica that cannot
      // see the journal at all refuses to build rather than book into a ledger that would
      // diverge silently. There is deliberately no `rewrite`: the DO compacts the one true
      // journal, and a replica rewrite would need a two-party barrier the wire does not have.
      const journal = durableSpendJournal();
      sharedLedger = createSpendReservationLedger({
        journal,
        salt: journal.salt,
        policy: sharedSpendPolicy(),
        // The lease this process holds is over the DO, not a file; the journal's own latch
        // (DurableSpendJournalError) is the ownership signal that matters here.
        assertOwnedAccounting: undefined,
        // The open reservations in this journal belong to the Durable Object and its still-live
        // Worker writers, not to a dead process -- resolving them as lost would double-book.
        reconcileReplayedSends: false,
      });
      return sharedLedger;
    }
    // Minted by the owner module from the directory it actually owns, and carrying the exact
    // ownership they were minted under. Nothing here chooses a path or supplies its own guard.
    const journalStorage = mintSpendLedgerStorage(SPEND_LEDGER_JOURNAL_FILENAME);
    const saltStorage = mintSpendLedgerStorage(SPEND_LEDGER_SALT_FILENAME);
    const journalPath = spendLedgerStoragePath(journalStorage);
    const saltPath = spendLedgerStoragePath(saltStorage);
    const assertOwnedAccounting = (): void => {
      assertStorageOwned(journalStorage);
      if (ledgerEntryExists(journalPath)) assertSafeLedgerFile(journalPath);
      if (ledgerEntryExists(saltPath)) assertSafeLedgerFile(saltPath);
    };
    sharedLedger = createSpendReservationLedger({
      journal: createOwnedFileSpendJournal(journalStorage),
      salt: loadOrCreateSpendLedgerSalt(saltStorage),
      policy: sharedSpendPolicy(),
      assertOwnedAccounting,
    });
  }
  return sharedLedger;
}

const MAX_DIAGNOSTIC_ERROR_COUNT = 1_000_000;

/** Scalar-only and side-effect-free: reading diagnostics never constructs or replays. */
export function spendLedgerDiagnosticsSnapshot(): {
  readonly ownership: "held" | "unheld";
  readonly initialized: boolean;
  readonly configured: boolean;
  readonly degraded: boolean;
  readonly persistFailures: number;
  readonly corruptRecords: number;
} {
  const ledger = sharedLedger;
  const bounded = (value: number): number => Math.min(MAX_DIAGNOSTIC_ERROR_COUNT, Math.max(0, value));
  return {
    ...spendLedgerOwnerSnapshot(),
    initialized: ledger !== undefined,
    configured: spendCeilingsConfigured(),
    degraded: ledger?.degraded ?? false,
    persistFailures: bounded(ledger?.persistFailures ?? 0),
    corruptRecords: bounded(ledger?.corruptRecords ?? 0),
  };
}

/**
 * Pull journal rows the Durable Object holds that this process has not applied and fold them
 * into the replica ledger, so a reservation a Worker turn just made bounds the admissions
 * this process makes next. No-op off Cloudflare and before the ledger exists; a compacted
 * DO journal rebuilds the ledger rather than import a checkpoint over divergent maps.
 */
export async function resyncSharedSpendLedger(): Promise<void> {
  if (!durableMirrorEnabled()) return;
  const journal = durableSpendJournal();
  // Before a ledger exists, the resync is the prefetch: the first sharedSpendLedger() call
  // needs a journal that has already answered.
  if (!sharedLedger) {
    await journal.prefetch();
    return;
  }
  const delta = await journal.resync();
  if (delta.kind === "unchanged") return;
  if (delta.kind === "reset") {
    // Replay of the DO's new journal replaces the replica whole: the checkpoint carries the
    // scope totals, and the reservations this process still owes an answer survive in
    // unconfirmed lines the journal keeps reporting.
    sharedLedger = createSpendReservationLedger({
      journal,
      salt: journal.salt,
      policy: sharedSpendPolicy(),
      assertOwnedAccounting: undefined,
      reconcileReplayedSends: false,
    });
    return;
  }
  for (const record of DurableSpendJournal.parseResynced(delta.lines)) sharedLedger.importRecord(record);
}

/**
 * Test seam for discarding the singleton outright.
 *
 * Production discards it too, but only with the ownership it belongs to, which is what
 * `onSpendLedgerOwnerReleased` does. Neither path resets a spent budget: the journal is the
 * durable record and the next construction replays it.
 */
export function resetSharedSpendLedgerForTest(): void {
  sharedLedger = undefined;
  setSharedSpendPolicy(DEFAULT_SPEND_RESERVATION_POLICY);
  resetSpendLedgerOwnerBindingForTest();
}

// Re-exported so existing catch sites keep the class they matched on before the split.
export { DurableSpendJournalError };

# Cloudflare storage — design (Phase 3 of Cloudflare-native hosting)

Status: design only, no code. Branch `feat/cloudflare-storage` off `dev` at `24b2f39b7`.
Depends on: the Cloudflare Containers deployment (PR #6077) for the Durable Object, the lease,
and the container-to-Worker state channel (`http://state.ocx.internal`).

## Problem

On Cloudflare the hub's disk disappears whenever the container sleeps or restarts. Phase 1 covers
this with whole-home tar snapshots every 30 seconds, plus one on `SIGTERM`. That leaves three gaps:

1. A crash loses up to 30 seconds of writes. For most state that is an inconvenience. For the
   spend ledger it breaks an invariant: a reservation must be durable before the request is
   admitted (`src/lib/spend-reservation-ledger.ts:1026`), so a crash can let a restarted hub
   overspend a budget it had already reserved against.
2. Every interval copies and hashes both homes whole. `usage.jsonl` is unbounded by default, so
   the cost grows without limit.
3. Nothing is queryable outside the container. Usage and routing history live in files inside a
   tarball.

The goal is to move the state a hub actually needs behind seams that can be backed by Durable
Object SQLite (or D1 and R2 where they fit better), store by store. Local installs keep their
files and see no behavior change.

## What a hub persists

From a full read of `src/` at `24b2f39b7`. Line references are to that tree.

| Store | Owner | Write | Read | Coordination today | Growth |
|---|---|---|---|---|---|
| `config.json` | `src/config.ts`, `config/persisted-mutation.ts` | `atomicWriteFile` under `withConfigMutationLockSync`; compare-and-swap retry | Loaded at startup, held in memory | `config-mutation.sqlite` `BEGIN IMMEDIATE` mutex and a generation counter | Bounded |
| `auth.json` (OAuth, all but the Codex pool) | `oauth/store.ts` | In-process FIFO, then an O_EXCL lock file, re-read, mutate, `atomicWriteFile` | **Re-read and parsed on every lookup** (`:829`) | Lock file with stale-steal after 120 s; dev/ino/mtime check before release | Bounded |
| Refresh intents `auth.refresh.*.lock{,.json}` | `oauth/store.ts:195-345` | O_EXCL lock; intent written before the upstream refresh, cleared by compare-and-swap | Per refresh | Stops two processes spending one rotating refresh token | One per account |
| `codex-accounts.json` (Codex pool) | `codex/account-store.ts` | Config mutation lock, `atomicWriteFile`, generation bump, tombstones | **Re-read per lookup** (`:278`) | Generation fence and the SQLite mutex | Bounded, keeps tombstones |
| `spend-ledger.jsonl` + `.salt` | `lib/spend-reservation-ledger.ts` | `appendFileSync` per record; compaction by rename | Replayed at startup, then in memory | One writer per home, proven by a process-lifetime SQLite lease (`spend-ledger-owner.sqlite`) | Bounded by compaction |
| `usage.jsonl` | `usage/log.ts` | `appendFileSync` per request, no lock | Tail readers keyed by inode and mtime | O_APPEND | **Unbounded by default** |
| `routing-history.sqlite` | `routing/history/*` | Incremental projection of `usage.jsonl` | Analytics | Tied to the source file's inode | Grows with usage |
| `responses-state.json` + `responses-state-spill/` | `responses/state.ts`, `spill-store.ts` | Debounced snapshot; spills via `linkSync` | Startup and continuations | One process | Capped (1,000 entries, 1 GiB) |
| Runtime caches (quota, reasoning metadata, replay) | various | Debounced `atomicWriteFile` | Warmed at startup | One writer; loss tolerated | Capped |
| `admin-api-token`, `service-api-token` | `server/management-auth.ts`, `lib/service-secrets.ts` | Written once (`linkSync` / atomic) | Startup | — | Tiny |

Local-only state (Codex shim and native profiles, tray, service manager, client links, Claude
intercept CA, desktop lifecycle locks) is out of scope: a Cloudflare hub never runs those paths,
and Phase 1 already leaves their lock databases out of snapshots.

## Constraints that shape the design

- **Everything is synchronous.** `withConfigMutationLockSync` forbids async callbacks; the
  reset-credit ledger asserts `Synchronous<T>`; OAuth and account lookups are synchronous file
  reads. A remote backend is asynchronous. Converting every caller to async is a repository-wide
  change on the core request path.
- **Coordination is built from filesystem semantics:** hard links as no-replace publication,
  rename atomicity, O_EXCL lock files with stale stealing, SQLite `BEGIN IMMEDIATE` as a mutex,
  inode and mtime as revision keys. None of these exist in a key-value or SQL service.
- **On Cloudflare there is exactly one process.** `max_instances: 1` and the Phase 1 lease
  guarantee a single writer per deployment. Almost all of the cross-process coordination above
  exists for a desktop machine running a service, a CLI, and a tray against one home. In the
  container it guards against a second process that cannot exist.

The third point is what makes this tractable.

## Approach: single-writer stores with durable write-through

Rather than a generic key-value layer under `atomicWrite*` (207 call sites, synchronous
semantics, filesystem tricks), each hub store gets a narrow backend interface, and a Cloudflare
backend that:

1. **Loads once at startup** from the Durable Object into memory, while the Phase 1 lease is held.
2. **Serves reads from memory.** This is safe because the lease makes this process the only
   writer. It also removes the per-lookup file re-reads of `auth.json` and `codex-accounts.json`.
3. **Writes through to the Durable Object.** Each store picks one of two durability classes:
   - *Must be durable before proceeding* (spend reservations; OAuth refresh-token rotation;
     account generation bumps): the write is awaited. The call sites that need this already sit in
     asynchronous request handlers, so the await is added at the handler boundary, not inside the
     synchronous helpers.
   - *May be written behind* (usage entries, caches, responses state): queued and flushed in
     order, with the queue drained on `SIGTERM` before the lease is released.

The file backend stays the default and keeps every existing lock, fence, and file format. The
backend is chosen once at startup (for example `OCX_STORAGE_BACKEND=cloudflare-do`, set only by
the Cloudflare supervisor), never per call.

Where Durable Object single-threading replaces a filesystem mechanism:

| Filesystem mechanism | Cloudflare equivalent |
|---|---|
| `spend-ledger-owner.sqlite` process-lifetime lease | The Phase 1 lease (one boot id holds the home) |
| `config-mutation.sqlite` mutex + generation | A Durable Object transaction that checks and bumps the generation |
| OAuth O_EXCL store lock and refresh-intent locks | Durable Object calls are serialized; the intent becomes a row written in the same transaction as the credential read |
| Hard-link no-replace publication | `INSERT` that fails on conflict |
| Inode/mtime revision keys for `usage.jsonl` tails | A monotonically increasing row id |

## Findings that changed the order (2026-09-27, from a full trace of the spend path)

- **Every hub journals every send.** The request tracker is attached unconditionally
  (`src/server/inference/context.ts:24`, `messages-native.ts:380`, `chat-native.ts:316`) and the
  first physical send appends a reserve record even with no `spend` ceilings configured, by design
  ("accounted and journalled"). A remote journal therefore means a network write on every request of
  every Cloudflare hub, not only those with limits.
- **The reserve path is synchronous end to end.** `SpendJournal.append` returns `void` and must throw
  on failure; `ledger.reserve` → `RequestSendObserver.charge` (`request-spend.ts:84`) →
  `RequestExecutionBudget.reserveDispatch` are all synchronous, with 11 `reserveDispatch` call sites,
  2 native `charge` sites, and about 13 more behind three synchronous helpers.
- **Admission is decided from in-process memory**; the journal is only replayed at construction. The
  local SQLite lease is what keeps a second writer out. On Cloudflare the Phase 1 boot lease already
  plays that role.

Consequences: making the reserve durable against a remote store before admission is either a wide
change to core send signatures (about 27 sites) or a narrower "durability barrier" awaited at the
three real send points (`adapters/physical-send.ts`, `chat-native.ts:460`, `messages-native.ts:485`),
plus a journal/salt injection point on `sharedSpendLedger` and a startup-time async replay. Either
way it touches the core request path, and it only closes a crash window of one snapshot interval
for hubs that configure ceilings. The spend ledger therefore moves from first to last, and should
be preceded by gating the tracker on `spendCeilingsConfigured()` so an unconfigured Cloudflare hub
makes no remote spend writes at all.

## Order of work

Each step is independently shippable and shrinks what the snapshot must carry. Items not yet
migrated keep riding in the Phase 1 snapshot.

| Step | Store | Why this order | Backend | Seam |
|---|---|---|---|---|
| 3a | OAuth `auth.json` + refresh intents, `codex-accounts.json` | Credential loss on crash forces re-login; per-lookup file reads are the hottest I/O | Durable Object SQLite, one row per provider account set / pool record, with the existing generation fields as the fence | New `CredentialStoreBackend` behind `oauth/store.ts` `loadStore`/`persist` and `account-store.ts` load/persist |
| 3b | `usage.jsonl` | Unbounded; biggest snapshot cost | Durable Object SQLite table (append rows, row id as revision), or D1 if cross-deployment queries are wanted | `appendUsageEntry` / `readRecentUsageEntries` in `usage/log.ts`; `routing-history` becomes a query, not a projection |
| 3c | `config.json` | Rarely written; snapshot already handles it well | Durable Object row + generation | `persisted-mutation.ts` commit path |
| 3d | Spend ledger | Only store with a durability invariant, but also the only one on every request's synchronous send path; first gate journalling on configured ceilings | Durable Object SQLite | `SpendJournal` plus a durability barrier awaited at the three physical send points |
| — | Caches, responses state, tokens | Loss is tolerated or they are regenerated | Stay in the snapshot, or are simply not persisted | none |

After 3a–3b the snapshot carries only config, the spend journal, and small caches, which removes gap 2, and the
interval can grow.

## Invariants each step must keep (and test)

- **Spend:** a send id is fully known or fully forgotten; under an enforced limit the reserve is
  durable before admission (`reserve-not-durable` otherwise); replay fails closed on corruption
  except a torn final record; the salt is stored with the ledger (losing it equals a reset).
- **OAuth:** `selectionRevision` rotates when an account set is replaced, so a rollback cannot
  restore an older generation; writes are fenced on `expectedGeneration`; a refresh intent is
  durable before the upstream refresh and cleared by compare-and-swap, so a rotating refresh token
  is never spent twice, including across a crash between refresh and write.
- **Codex pool:** `record.generation === dispatched.generation` for live checks; tombstones are kept.
- **Usage:** entries are append-only and ordered; readers resume from a revision key.
- **Fencing:** a container that has lost the lease must not commit to any store. Every Durable
  Object write carries the boot id, and the object rejects writes from a non-holder, as the
  Phase 1 snapshot commit already does.

## Testing

- Each backend interface gets a shared conformance suite run against both the file backend and an
  in-memory fake of the Durable Object backend (the pattern Phase 1 used for `LeaseState`).
- Crash tests: kill between upstream refresh and credential write; between reserve and admission;
  mid-flush of the write-behind queue.
- Fencing tests: a fenced boot's writes are rejected by every store.
- A `wrangler dev` end-to-end run per step, and one production-account run before calling a step
  done, as Phase 1 did.

## Risks and open questions

- **Scope creep into async conversion.** The design only adds awaits at request-handler
  boundaries. If a store's durable write turns out to sit under a synchronous helper with no
  async caller, that store is deferred rather than forcing an async cascade.
- **Latency.** A durable spend reservation adds a Worker-to-Durable-Object round trip on every
  admitted request under an enforced limit. It needs measuring; unlimited users skip it.
- **Durable Object limits.** 2 MB per row and 10 GB per object. Credentials and ledgers are far
  below this; usage needs retention (the existing `usageLedgerMaxBytes`) or D1.
- **Two sources of truth during migration.** A store is either fully on the Durable Object or
  fully in the snapshot, never both; the snapshot excludes migrated files explicitly.
- **Maintainer questions:**
  1. Is a startup-selected backend acceptable, or should the seams be introduced without any
     Cloudflare backend first, as a pure refactor?
  2. Should usage go to D1 (queryable across deployments, extra binding) or stay per-hub in the
     Durable Object?
  3. Is the spend-ledger latency cost acceptable, or should Cloudflare hubs with enforced limits
     keep a local durable append plus asynchronous replication?

## 3a progress (2026-09-27): OAuth `auth.json` written through

Smaller than the seam in the table: the file stays the store every reader uses, and the Durable
Object gets a whole-document copy on each commit. The first version failed the commit when the
Durable Object was unreachable; both adversarial reviewers showed that discards a refresh token the
provider had already rotated, which is worse than no mirror. The shipped rule is that the local
write always happens (except after a lost lease), and a sequence number decides which copy is newer.

- `mutateStore` (`src/oauth/store.ts`) is the only writer of `auth.json`. With a boot id it awaits
  `mirrorBeforeWrite("auth", …)` (`src/lib/durable-mirror.ts`) before `persist`, re-runs `assertBeforePersist`
  after that await, then records `{seq, mirrored}` in `auth.json.seq`. Without a boot id nothing is
  awaited, so local installs keep their synchronous check-then-write.
- Two attempts of 5 s each keep a commit under the 30 s mutation-queue wait and lock staleness. A
  failure schedules a catch-up retry of that same document and sequence; a newer commit cancels it.
- The Durable Object stores `{body, seq}` under the lease and answers 412 with its sequence to any
  write that is not newer, so a timed-out attempt that lands late cannot replace a later commit. A
  commit (never a catch-up) told of a higher stored sequence moves past it, because the committing
  process holds the lease and the newest store.
- The supervisor restores the Durable Object copy unless the snapshot's `auth.json.seq` says
  `mirrored: false` with a higher sequence. `OCX_DISCARD_SAVED_STATE` deletes the document.
- `OCX_STATE_BOOT_ID` is inherited by the container's whole `ocx` process tree, not only `ocx`;
  nothing else there reads it.

Known gaps, all narrower than the snapshot-only behavior they replace:
- A login rejected by the second `assertBeforePersist` has already been mirrored; the unchanged
  local store is committed over it as the next sequence. If that revert cannot reach the Durable
  Object, the sequence file records the local store as newer and unmirrored, so restore keeps it.
- The sequence file also records a digest of the content its sequence names, and snapshots stage
  each sequence file before its document. A local file that no longer matches its digest was
  changed by something that did not record it, so restore keeps it. That closes the staging race
  noted earlier and the rollback of writes made by an older image.
- Writes from a separate `ocx` process in the container (for example `ocx login` over a shell)
  have no boot id and are not mirrored; the digest check keeps them at restore, but they reach the
  Durable Object only with the next mirrored commit.

`codex-accounts.json` followed (same day). Its writers are synchronous and run inside the SQLite
config-mutation lock, so they cannot await the network. `writeCodexAccountsFile` in
`src/codex/account-store.ts` is now its only writer (`orca-import.ts` included): it marks the next
sequence `mirrored: false` before the local write, then mirrors in the background with the same
retry and supersede rules. A crash anywhere before the Durable Object accepts the copy leaves the
next boot preferring the local file; a crash before the local write lands loses only what the
snapshot-only design lost. The mirror module moved to `src/lib/durable-mirror.ts` and names its
documents in `DURABLE_DOCUMENT_FILES`, which a test holds equal to the Worker's allowlist.

Coupling left open until 3c: pool account add/remove writes `config.json` (still snapshot-only)
and `codex-accounts.json` together, so a death without `SIGTERM` inside one interval can restore
them out of step. Token refresh, the case this step exists for, touches only `codex-accounts.json`.
A pool large enough to pass the 1 MiB document cap (about 200 accounts) stays snapshot-only with a
size warning per write.

Still riding the snapshot: refresh intents (`auth.refresh.*.lock.json`, Codex refresh locks, Nous
intents) and the `pre-multiauth` backup. Losing an intent in a crash is the same outcome as the
crash itself today.

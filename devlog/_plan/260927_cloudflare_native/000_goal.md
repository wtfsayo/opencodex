# Cloudflare-native hosting — long-term goal

Status: open. Phase 1 implemented on `feat/cloudflare-hosting`; later phases are plans.

## Goal

An operator can run an opencodex hub on Cloudflare alone — no VPS, no home machine, no
Tailscale — and each phase moves more of that hub from "a Linux box Cloudflare rents us" to
Workers primitives, without forking the local CLI product.

## Why staged, not a rewrite

Measured on `dev` at 429f4e017: `src/` is ~450k lines; 306 files import `node:fs`, 90 spawn
processes, 33 open `bun:sqlite`, 40 call sites use `Bun.serve`. On Workers, `node:fs` is an
in-memory per-request filesystem, `child_process` is a no-op stub, there is no Bun API, and the
isolate has 128 MB. A rewrite would take months and leave two products. The translation core
(Responses <-> Messages <-> chat SSE over `fetch` and streams) is the part that ports well, so it
moves last and one route at a time.

## Platform facts this plan depends on (checked 2026-09-27)

| Fact | Source |
|---|---|
| Containers GA 2026-04-13; Worker -> Container DO -> VM; `fetch()` forwards WebSockets | developers.cloudflare.com/containers |
| Container disk is ephemeral: a slept or rolled-out instance restarts from the image | containers/platform-details/architecture |
| `outboundByHost` handlers run in the Workers runtime with all bindings; plain HTTP to an invented host is intercepted; `ContainerProxy` must be exported | containers/platform-details/outbound-traffic |
| Rollouts send SIGTERM and wait `rollout_active_grace_period` before SIGKILL | wrangler config schema 4.141.0 |
| Workers: no wall-clock limit on streamed responses, CPU billed only | workers/platform/limits |
| Access can gate a Worker; identity arrives as `Cf-Access-Jwt-Assertion` | workers/configuration/cloudflare-access |
| R2 FUSE (tigrisfs) needs FUSE privileges and has no hard links | containers/examples/r2-fuse-mount |

The last row is why Phase 1 snapshots instead of mounting: `src/config/initialize.ts` publishes the
first config with a hard link, and SQLite on an object-store filesystem is unsafe.

## Phases

Revised 2026-09-27: a Cloudflare Access sign-in (verified `Cf-Access-Jwt-Assertion` as a GUI-session
source) was built and reviewed, then set aside because the operator does not want the dashboard to
depend on Cloudflare identity. It is parked on the local branch `feat/cloudflare-access`.

| Phase | Outcome | Exit criterion |
|---|---|---|
| 1 Container hub | Unmodified Docker image runs as a Cloudflare Container; state survives sleep and rollout via R2 snapshots fenced by a DO lease; data plane on the Worker URL | `wrangler deploy`, `curl /healthz`, Codex routed through `/v1` with a data token, state survives `wrangler deploy` of a new version |
| 2 Dashboard | `gui/dist` served by the Worker as static assets (absorbs the former Phase 4), signed in with the operator's own admin token; no Cloudflare Access dependency | Done on `feat/cloudflare-dashboard`: live browser sign-in, Online status, every `/api` call 200, container never started by page loads |
| 3 Storage port | File/SQLite state behind an interface with a DO-SQLite/D1 implementation, selected at startup | Snapshot window gone for the migrated stores; local installs unchanged. Done for `auth.json`, `codex-accounts.json` and `config.json` (write-through mirror, `feat/cloudflare-credentials` and `feat/cloudflare-config`) and the usage ledger bounded (`feat/cloudflare-usage`); spend ledger deferred, see `260927_cloudflare_storage/000_design.md` |
| 4 Worker-native data plane | Stateless translation routes run in the Worker behind `run_worker_first`, container as fallback; an import-graph test (modelled on `tests/lab/core-lab-boundary.test.ts`) keeps them free of Bun/`node:fs` | p50 latency and CPU-ms per streamed turn measured before and after |
| 5 Container optional | Container starts only for features that need a process (sidecars, CLI-backed providers) | A Worker-only deployment serves API-key providers with the container never started |

## Phase 1 design

- `deploy/cloudflare/` is a self-contained Wrangler package. It changes no `src/` file, so the
  runtime's own admission (`src/server/auth-cors.ts`) stays the only authority; the Worker passes
  every request through.
- The image is the root `Dockerfile` `runtime` stage, unchanged. The Container class overrides the
  entrypoint with `docker/cloudflare-supervisor.ts`.
- One named instance (`max_instances: 1`), because the spend ledger and several stores assume one
  writer per `OPENCODEX_HOME`.
- Supervisor: acquire the DO lease -> restore the R2 snapshot -> start `ocx` -> every interval renew
  the lease and upload a snapshot when content changed -> on SIGTERM stop `ocx`, upload a final
  snapshot, release the lease. Losing the lease stops `ocx` without uploading (fencing).
- Snapshots copy SQLite databases with `VACUUM INTO`, skip `-wal`/`-shm`/`-journal` sidecars and
  lock/owner databases, and copy other files as-is (they are written atomically).
- Secrets: `OPENCODEX_API_AUTH_TOKEN` (required, the Worker fails closed without it),
  `OPENCODEX_ADMIN_AUTH_TOKEN`, and `OCX_BOOTSTRAP_CONFIG_JSON` (first boot only).

## Phase 1 findings from `wrangler dev` runs (2026-09-27, local, amd64 under emulation)

| Observation | Consequence |
|---|---|
| `@cloudflare/containers` 0.3.7 counts port readiness in tries, not time; with the port closed for a 120 s lease wait, the Durable Object reached `healthy` ~3 min after `ocx` listened, and requests hung until then | The supervisor serves a 503 placeholder on :10100 from the first instant and hands the port to `ocx` after restore. Readiness is immediate; recovery requests fail fast with `Retry-After: 10` |
| `pingEndpoint` is used as `http://${pingEndpoint}`, i.e. host + path | `pingEndpoint = "localhost/healthz"` |
| A per-boot snapshot key let a fenced container's late upload overwrite, then delete, the object the new holder restored from | One key per upload; regression test drives the race and fails on the per-boot key |
| Evidence: `/healthz` 200; `/v1/models` 401 without token, 200 with; SIGTERM -> final upload -> restore of a marker file and a CLI config change; SIGKILL -> 503s then 200 at 81 s with state intact; WebSocket 101 through the Worker once `websockets` was enabled; missing data-token secret -> 503; R2 holds exactly one snapshot object | Not yet run against real Cloudflare (needs an account deploy) or a real provider turn |

## Adversarial review (two reviewers, 2026-09-27) and resolution

| Finding | Resolution |
|---|---|
| `rollout_active_grace_period` is a minimum connection age, not a flush budget; rollouts stop old before new, with 15 min SIGTERM->SIGKILL | Setting removed; comments and guide corrected |
| Periodic and final uploads could commit out of order | All uploads run through one promise chain; shutdown awaits it |
| Failed final upload exited without release; failed restore crash-looped holding the lease | Final upload retries with backoff; lease always released; restore failure releases and refuses to start `ocx` on an empty home |
| `.opencodex-native-main.claim.sqlite` escaped the lock-DB pattern | `claim` added |
| SIGTERM ignored before restore finished (bun is PID 1) | Handlers registered first; verified: stop during lease wait exits in <1 s |
| Missing R2 object read as first boot | 500, never 404 |
| `/api/*` reachable with the runtime-generated admin token, which was also snapshotted | Worker returns 404 for `/api/*` unless `OCX_EXPOSE_MANAGEMENT_API=1` and an operator admin token; `admin-api-token` excluded from snapshots |
| Rotated token stayed valid while the old container ran | Container restarts on the next request when its secret fingerprint changes (typechecked + unit-tested inputs; restart path NOT exercised live: `wrangler dev` does not reload `.dev.vars`) |
| Any process in the container could download the snapshot with a well-formed boot id | `GET /snapshot` requires the lease holder |
| Anonymous traffic woke and billed the container | Worker 401 on requests with no credential header (presence only; `ocx` still authenticates) |
| `deploy/**` outside CI path filters | Added to both lists and the pinned test |

Open follow-ups: no CI job typechecks `deploy/cloudflare` (needs a dependency install step, a
workflow security review item); periodic snapshots copy whole homes each interval and will grow
with `~/.codex` sessions; the pairing rate limiter keys on the peer IP, which is Cloudflare's for
every client (moot while `/api/*` is closed); CORS preflights and audio key subprotocols pass the
edge presence check (re-review), so an anonymous preflight can still wake the container.

## Known Phase 1 limits

- A crash (not a SIGTERM) loses writes since the last snapshot, bounded by the interval.
- The dashboard cannot mint a remote session on this listener; that is Phase 2.
- Cold start after sleep includes lease + restore time.
- Deploy button: Cloudflare's docs do not list Containers among auto-provisioned resources, so
  Phase 1 documents `wrangler deploy` only.

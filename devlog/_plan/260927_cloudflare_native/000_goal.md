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

## Phase 4a (2026-09-28): Worker-native streamed Chat Completions, opt-in

Baseline (test deployment, Workers AI `meta/llama-3.1-8b-instruct-fp8`, 20 streamed turns per
route, container path): p50 time to first byte 651 ms (chat) / 669 ms (responses), p50 total
793 / 831 ms. The container's outbound call to Workers AI alone had p50 wall 471 ms, so the
Worker -> Durable Object -> container -> ocx hop cost roughly 300 ms per turn. Worker CPU was
1-2 ms. Raw rows are in a scratch SQLite database, not committed.

Shape:
- `src/server/cloudflare-native-chat.ts` reuses ocx's `buildOpenAIChatPassthroughRequest` and
  `nativeChatSse` unchanged. Routing is a narrow subset (exact `<provider>/<model>`, allowlisted
  provider fields, no routing sections) that a test holds equal to `routeModel` where it answers.
  Anything else, and any upstream error, declines to the container before a byte is sent.
- It lives under `src/` because the ocx modules it reuses do not typecheck against Workers types;
  the Worker package sees it through `native-chat-api.ts` and a wrangler `alias`.
- `tests/service/cloudflare-worker-native.test.ts` walks its import graph (dynamic imports and
  `.js` specifiers included) and fails on Bun APIs, `node:` modules other than `buffer` and
  `crypto` (enabled by `nodejs_compat`), ocx's stateful owners, or growth past 80 files. Getting
  under it took one move: `modelRecordValue` out of `reasoning-effort.ts` into `lib/model-record.ts`.
- The supervisor now publishes a local document the Durable Object lacks at boot, so a fresh
  deployment's config is readable by the Worker without a settings save.

Not done: usage rows and request logs for Worker-served turns (next: queue them in the Durable
Object for the container to ingest), Responses and Messages routes, non-streamed turns.

Security review (same day) found the first cut unsafe and it was withdrawn from the test
deployment until fixed: a provider named after a built-in one had its configured `baseUrl` used
where ocx substitutes the registry endpoint (the key could go to a stale or hostile URL); ocx's
private-destination check was skipped; `blockedModelRedirects` was ignored; the edge's any-header
key check admitted forms ocx refuses on chat; and bodies were read without a cap. Fixes: a
top-level config key allowlist instead of a section denylist, built-in provider names declined,
https public host names only (plus the Worker's own hosts), ocx's chat header rule
(`chatAdmitsDataToken`), a 4 MiB cap checked before reading, compressed bodies left to ocx, and an
aborted client no longer wakes the container.

Measurement. Sequential runs were useless: the container path's own p50 moved from 651 to 924 ms
between two runs an hour apart, so any before/after pair mostly measured Workers AI and this
machine's load. The number that counts is an interleaved A/B in one run: the same streamed turn,
alternating a plain request (served in the Worker) with the identical request plus
`content-encoding: identity` (declined by the Worker, served by the container; ocx treats identity
as a no-op). 30 turns per arm, 2026-09-28:

| Arm | p50 first byte | p50 total | p90 first byte |
|---|---|---|---|
| Worker-native | 447 ms | 582 ms | 492 ms |
| Container | 790 ms | 938 ms | 906 ms |

That is about 340 ms (43%) off each turn, most of it the Durable Object and container hop rather
than Workers AI. The A/B also caught a silent failure: the first run after the security fixes
showed no difference because the key allowlist declined every turn (a real hub's config carries
`runtimeRole`, `hub`, `fastRows`, `subagentModels`), which is why the Worker now logs each decline
reason once. Worker CPU stays at 1-2 ms per turn.

Open after the final review (all low): upstream rewriter plugins under `$OPENCODEX_HOME/plugins`
are not run by the Worker and not declined (not reachable on Cloudflare today); the import guard
misses non-literal `import(x)` and its comment stripping ignores strings; CI never bundles the
Worker, so a module-scope incompatibility in the reused code shows up only at deploy time. Fixed
from that review: redirects are `manual` as in ocx, a 200 s header timeout as ocx's default,
and empty `messages` declined. `appOwnedMemoryBudgetMb` was removed from the allowlist, which made
the test hub (whose config sets it) decline every turn; tracing showed no module on the native chat
lane consults it (it bounds the container process's retained state), so it was allowed again.

## Phase 5 progress (2026-09-28): Worker-only serving

Branch `feat/cloudflare-worker-only`, stacked on `feat/cloudflare-worker-native`.

- Config: the Worker routes with the Durable Object's copy, or `OCX_BOOTSTRAP_CONFIG_JSON` before
  the container has ever run, and resolves `${NAME}` keys against `containerEnv(env)`, the exact
  environment the container would get.
- Chat: non-streamed turns too, following chat-native.ts from the upstream response on.
- Usage: rows queued in the Durable Object (20,000 max), drained into usage.jsonl by ocx at startup
  and each minute through appendUsageEntry; at-least-once.
- Responses: `/v1/responses` for openai-chat providers, reusing `parseRequest`, the openai-chat
  adapter and `bridgeToResponsesSSE`. Getting there took moving disk-backed state out of their
  import graphs without changing proxy behaviour: a thought-signature slot the disk store registers
  into, the replay-prefix WeakMap in its own module, the adapter taking effort mapping and image
  normalization as dependencies, and `buildToolBridgeMaps` in its own module. An exploration
  traced the container's path for eligible turns (request-prepare, core-normalize, dispatch,
  delivery); the Worker reproduces its pure steps (routed model id, identity rename,
  hideThinkingSummary) and declines where it is stateful (skills snapshot, stored responses,
  effort ladders, collaboration, code mode, namespaces, images). Both guards are off for these
  turns (terminal: provider opt-in; empty completion: config opt-in).
- Not reproduced at first: `/v1/models` (upstream discovery and the Codex catalog template),
  `/v1/messages`, WebSocket Responses. Those started the container.

Exit criterion met (2026-09-28): a fresh deployment (`opencodex-worker-only-test`, configured only
by secrets: data token, a bootstrap config with the Workers AI provider, `OCX_WORKER_NATIVE=1`)
served streamed and non-streamed chat and streamed Responses turns, 9 of 9 with status 200, and
refused a wrong token with 401. The Worker log showed only `readDocument` and `enqueueUsage`
calls into the Durable Object, no container fetch and no state-host traffic (which a booting
supervisor makes at once), and the deployment's R2 bucket held 0 objects (a running container
uploads a snapshot within 30 s). `wrangler containers instances` lists the `hub` object as
inactive with no location or version. The first deploy of the Responses path failed at upload:
`reasoning-replay-cache.ts` drew `randomBytes(32)` at module scope, which Workers refuse; the key is
now created on first use, and the import guard flags module-scope random, timers and I/O.

Real Codex CLI (0.157.1, `codex exec` with an isolated CODEX_HOME, provider set by `-c` overrides,
wire_api responses) against a Worker-only deployment, 2026-09-28. It took four fixes found only by
running the real client: Codex sends `client_metadata`, a `multi_agent_v1` namespace tool, hosted
`{type:"web_search", external_web_access:false}` and a `<skills_instructions>` block on every turn,
and the Worker declined each in turn. With those reproduced (namespaces via ocx's own flattening,
web_search admitted when no `openai` provider can run the search sidecar, the skills freeze kept in
the Durable Object), a plain turn and a full tool loop with `meta/llama-3.3-70b-instruct-fp8-fast`
(model calls the shell tool, Codex runs `wc -l`, model answers "2 lines") were served with only
Worker events and Durable Object calls (`nativeConfigSource`, `skillsSnapshot`, `enqueueUsage`) in
the log. Llama 4 Scout answered Codex's full tool set with a tool call written as text, a model
limitation. Codex CLI made no `/v1/models` request in these runs.

Messages (branch `feat/cloudflare-worker-messages`, 2026-09-28): `/v1/messages` is served by
translating to a Responses request and running the Worker's Responses turn with the inbound wire
set to `anthropic` (no hideThinkingSummary recompute, declared tool names not enforced, as
run-turn-execution.ts does), then `responsesSseToAnthropicSse` with ocx's own input-token floor.
Only `ocx-claude-*` aliases qualify: a bare Claude id can be native passthrough, a Claude Desktop
alias, a modelMap entry or a classifier check, all resolved against state the Worker lacks. The
translator closure had to shed Claude Desktop state first (alias-codec, one-m-marker, a Desktop
lookup slot); the Worker's import closure went from 129 to 150 files, all translation modules.
Tests run the same turn through `handleClaudeMessages` and the Worker and compare the upstream
body and the client bytes.

Real Claude Code (2.1.283, `claude -p` with an isolated CLAUDE_CONFIG_DIR, `ANTHROPIC_API_KEY` set
to the data token) against `opencodex-wo2-test`: a plain turn (Llama 4 Scout, 5.0 s) and a two-turn
Bash tool loop (`qwen/qwen3-30b-a3b-fp8`, 9.4 s, "2 lines") were served with only
`nativeConfigSource` and `enqueueUsage` in the Durable Object log. Three model limits surfaced,
each reproduced identically by the container after the Worker declined the upstream error:
Llama 3.3 70B's 24k window is smaller than Claude Code's 32,000 `max_tokens` and its 16k-token
prompt; Scout writes tool calls as text; Mistral Small 3.1 refuses the `system` message Claude
Code inserts after a tool result ("Unexpected role 'system' after role 'tool'"). The last one is
ocx's openai-chat translation, not the Worker.

Still container-only: `/v1/models`, WebSocket Responses, the dashboard and management API, OAuth
and Codex-pool providers, and effort on destinations with models.dev metadata (OpenCode Zen).

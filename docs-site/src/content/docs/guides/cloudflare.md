---
title: Cloudflare Deployment
description: Run an opencodex hub on Cloudflare Workers, Containers, Durable Objects, and R2, with state that survives sleep and redeploys.
---

`deploy/cloudflare/` runs the same image as [Docker Compose](/guides/remote-hub/#docker-compose)
on Cloudflare, with no server of your own. A Worker receives every request and forwards it to one
[Cloudflare Container](https://developers.cloudflare.com/containers/) running `ocx`. The container
disk is wiped whenever the container sleeps or a new version rolls out, so the hub's state lives in
an R2 bucket between runs.

```text
client ──HTTPS──▶ Worker ──▶ OpencodexHub (Durable Object) ──▶ container: supervisor ─▶ ocx :10100
                                   │ lease                              │ snapshots
                                   └──────────────── R2 bucket ◀────────┘
```

:::note[Status]
This is the first stage of Cloudflare support. `ocx` still runs as a Linux process inside a
container; it is not yet a Workers-native runtime. Clients call `/v1/*` with a key; the dashboard
is available when you open it with your own admin token (see [Dashboard](#dashboard)). It has been
exercised with `wrangler dev` and on a production Cloudflare account, including a real browser
sign-in to the dashboard and real model turns through [Workers AI](#workers-ai-no-api-key).
:::

## Requirements

- A Cloudflare account on the Workers Paid plan, which Containers require.
- Docker running on the machine you deploy from. `wrangler deploy` builds the root `Dockerfile`
  there and pushes the image to Cloudflare's registry.
- Deploy from a normal clone. In a `git worktree`, `.git` is a file rather than a directory, so the
  image build cannot read the index; run `bun scripts/generate-compatibility-version.ts` first.
- Bun, to install the Wrangler package.

## Deploy

```bash
cd deploy/cloudflare
bun install
npx wrangler login
npx wrangler r2 bucket create opencodex-state

# The data-plane token clients will send. History records the command, not the value.
export OPENCODEX_API_AUTH_TOKEN="$(openssl rand -hex 32)"
printf '%s' "$OPENCODEX_API_AUTH_TOKEN" | npx wrangler secret put OPENCODEX_API_AUTH_TOKEN

npx wrangler deploy
```

Save the value of `$OPENCODEX_API_AUTH_TOKEN` in your password manager now; Cloudflare will not show
it again, and the steps below use it. Until the
`OPENCODEX_API_AUTH_TOKEN` secret exists, the Worker answers every request with `503` and names the
missing secret.

Check the deployment with the token in a header:

```bash
curl -H "x-opencodex-api-key: $OPENCODEX_API_AUTH_TOKEN" \
  https://opencodex.<your-subdomain>.workers.dev/healthz
```

The Worker answers `401` to any request that does not carry the data token, without starting the
container, so scanners and wrong keys cannot keep it running. It reads the token from the same places
`ocx` does: `x-opencodex-api-key`, `Authorization: Bearer`, `x-api-key`, or, for the audio WebSocket,
the key subprotocol. The comparison takes the same time whatever the key. `ocx` still checks every
key it receives.

The Worker only knows the data token. If you issue further client keys through `apiKeys` in the
hub's configuration, the Worker would refuse them, so store `OCX_EDGE_KEY_CHECK=presence` as a secret
to go back to checking only that some key is present. That costs money: any request with any
non-empty key then starts the container and keeps it awake, and only `ocx` rejects the wrong ones.

`OPTIONS` requests are answered by the Worker without CORS headers, so cross-origin browser requests
that need an authentication header fail. The audio WebSocket still works from a browser, because it
carries its key as a subprotocol. Because `/healthz` needs a key too, an uptime monitor must be given
the data token, or, with `OCX_EDGE_KEY_CHECK=presence`, a dedicated client key.

The first request starts the container, which takes a few seconds. Requests that arrive while it is
restoring or saving state get `503` with `Retry-After: 10`.

## Configure providers

On first boot, with no saved state, the hub uses the image's default configuration. To start from
your own `config.json` instead, store it as a secret before the first request:

```bash
npx wrangler secret put OCX_BOOTSTRAP_CONFIG_JSON < config.json
```

The hub must listen on `0.0.0.0:10100`, the only address the Worker reaches. Leave `hostname` and
`port` out of the file and they are filled in; any other value stops the first boot with an error,
before an unreachable configuration can be saved. Worker secrets have a size limit, so keep the file
small. The secret is read only when no saved state exists. After that, the saved configuration wins;
see [Change configuration later](#change-configuration-later).

Keep provider API keys and `apiKeys` entries out of the file, because the configuration is saved to
R2. Reference provider keys as `${NAME}` in the provider's `apiKey`, as in
[Providers](/guides/providers/), store each as a secret, and list the names in
`OCX_PASSTHROUGH_SECRETS`:

```bash
npx wrangler secret put ANTHROPIC_API_KEY
printf 'ANTHROPIC_API_KEY' | npx wrangler secret put OCX_PASSTHROUGH_SECRETS
```

Only names listed there reach the container.

## Change configuration later

Once a snapshot exists, a new `OCX_BOOTSTRAP_CONFIG_JSON` is ignored. There are two ways to change
the configuration after that:

- **Keep the saved state.** Open the management API for the change, as described under
  [Security](#security), and use it like any other hub. Close it again afterwards.
- **Start over from the bootstrap secret.** Store the new `OCX_BOOTSTRAP_CONFIG_JSON`, then set
  `OCX_DISCARD_SAVED_STATE` to a value it has never had; a random one is safest. This discards the saved state:
  OAuth logins, client keys, and usage history go with it.

  ```bash
  npx wrangler secret put OCX_BOOTSTRAP_CONFIG_JSON < config.json
  openssl rand -hex 8 | npx wrangler secret put OCX_DISCARD_SAVED_STATE
  ```

## Workers AI (no API key)

The Worker has a Workers AI binding, so the hub can use Cloudflare's own models with no provider key;
usage bills to the account that owns the deployment. Point an `openai-chat` provider at the Worker's
internal address and name models without the `@cf/` prefix:

```json
{
  "providers": {
    "workers-ai": {
      "adapter": "openai-chat",
      "baseUrl": "http://ai.ocx.internal/v1",
      "apiKey": "workers-ai-binding",
      "models": ["meta/llama-3.1-8b-instruct-fp8"]
    }
  },
  "defaultProvider": "workers-ai"
}
```

`ai.ocx.internal` exists only inside the container; the Worker answers it through the binding. The
`apiKey` value is not checked. The shim carries text chat, streamed or not, and function tools for
models that support function calling, such as `meta/llama-3.3-70b-instruct-fp8-fast` and
`meta/llama-4-scout-17b-16e-instruct`; those are the ones to use with Codex, which always sends
tools. It honours `tool_choice: "auto"` only and refuses images and other `tool_choice` values
rather than dropping them. Workers AI retires models over time; list current ones in
the dashboard under **AI → Workers AI → Models**.

## Worker-native requests (experimental)

Setting `OCX_WORKER_NATIVE` to `1` lets the Worker answer some `/v1/chat/completions` requests
itself, without the container. That removes the Worker-to-container hop from each turn
and lets those turns run while the container is asleep. It is off by default.

```bash
echo 1 | npx wrangler secret put OCX_WORKER_NATIVE
```

The Worker serves a request only when all of these hold, and otherwise passes it to `ocx` unchanged:

- The request is `POST /v1/chat/completions`, streamed or not, with text-only messages and at
  most `function` tools; its body is uncompressed and under 4 MiB; and it carries the data token in
  `x-opencodex-api-key`, or failing that as a bearer token, which is how `ocx` reads it for chat.
  With `OCX_EDGE_KEY_CHECK=presence` the Worker never serves a request, and a request that carries
  an `Origin` header (a browser) always goes to `ocx`, which applies its own origin rules.
- `model` is `<provider>/<model>` for a provider you added yourself (not a built-in provider name
  such as `openai` or `deepseek`), whose config has only `adapter: "openai-chat"`, an `https`
  `baseUrl` on a public host name, a literal `apiKey`, `models` (which must list the model), and
  optionally `authMode: "key"`, and a `baseUrl` other than OpenCode Zen's, whose reasoning levels
  `ocx` looks up in models.dev. The key can be a literal or a `${NAME}` reference to a secret listed
  in `OCX_PASSTHROUGH_SECRETS`, as `ocx` would resolve it. Workers AI qualifies as shown above.
- The config has nothing beyond basic settings: any routing, redirect, limit, or surface section
  sends every request to `ocx`.
- The turn is not a multi-agent collaboration turn (a `spawn_agent` tool, or a spawned child's
  headers), which `ocx` gives a reasoning-effort cap, and the model id has no `--`, which `ocx`
  reads as a Fast or effort row.

When the Worker passes a request on, it logs why once per reason (for example
`Worker-native chat declined: config-keys:<names>`); `npx wrangler tail` shows it.

It also answers `POST /v1/responses`, the API Codex uses, for the same providers when the turn is
streamed, not stored (`"store": false`, as Codex sends it), and uses only
`function` tools, grouped in namespaces or not, plus hosted `web_search` when no `openai` provider
is configured (ocx's search sidecar runs through it); no code-mode `exec` or custom tools; text-only
messages; and no `<skills_instructions>` block. That covers what Codex CLI sends on an ordinary
turn. Like `ocx`, the Worker freezes each session's `<skills_instructions>` catalog to the first one
it sends, for four idle hours; the two keep separate copies (the Worker's in the Durable Object,
`ocx`'s in memory, lost whenever the container sleeps), so a session whose catalog changes while its
turns alternate between them can see both versions.

If the provider returns an error status, the Worker sends the request to `ocx` instead, which
retries and reports it as usual; the provider then sees that request twice. An answer with a 200
status is relayed or reported by the Worker itself, as `ocx` would.

Turns the Worker serves are queued in the Durable Object and added to usage history when `ocx`
next runs (at startup, then every minute), so the Usage page shows them once the container is up.
The queue holds the latest 20,000 turns. They do not appear in request logs, and they do not count
toward spend ceilings (a config with a `spend` section is never served by the Worker). The Worker reads the provider settings from the copy of
`config.json` kept in the Durable Object, which the container updates whenever settings change;
until the container has run once, it uses `OCX_BOOTSTRAP_CONFIG_JSON` as the container would
seed it. A hub upgraded from a version without this copy serves nothing from the Worker until the
container has started once and published its settings. So a deployment whose
clients only make qualifying requests never starts the container at all.

## Connect Codex

Point Codex at the hub with the data token in an environment variable. In `~/.codex/config.toml`:

```toml
model_provider = "opencodex"

[model_providers.opencodex]
name = "opencodex"
base_url = "https://opencodex.<your-subdomain>.workers.dev/v1"
wire_api = "responses"
requires_openai_auth = true
env_key = "OPENCODEX_API_AUTH_TOKEN"
```

This is the table `ocx` itself writes for a remote hub. Export `OPENCODEX_API_AUTH_TOKEN` in the
shell that starts Codex.

## Dashboard

The Worker serves the dashboard's files itself, so opening it never starts the container. It is off
until you choose an admin token:

```bash
export OPENCODEX_ADMIN_AUTH_TOKEN="$(openssl rand -hex 32)"   # save this value
printf '%s' "$OPENCODEX_ADMIN_AUTH_TOKEN" | npx wrangler secret put OPENCODEX_ADMIN_AUTH_TOKEN
printf '1' | npx wrangler secret put OCX_EXPOSE_MANAGEMENT_API
```

Set `hub.managementPublicOrigin` to the Worker's URL (for example in `OCX_BOOTSTRAP_CONFIG_JSON`),
then open `https://opencodex.<your-subdomain>.workers.dev/`. The dashboard asks for the admin token
and keeps it only in page memory, so it asks again after a reload.

- `wrangler deploy` builds the dashboard first (`bun run build:gui`); `gui/dist` is uploaded as
  static assets.
- On `/api/*` the Worker accepts only the admin token; a data token there is refused before the
  container starts. `ocx` checks the admin token again.
- A keyless `GET /healthz`, which the dashboard polls for its status badge, returns the hub's health
  while the container runs and `503 {"status":"sleeping"}` otherwise, without starting it.

## How state is kept

The container's entrypoint is `docker/cloudflare-supervisor.ts`. It:

1. Takes a lease from the Durable Object, so only one container writes state at a time.
2. Restores `~/.opencodex` and `~/.codex` from the latest snapshot in R2, then restores `auth.json`,
   `codex-accounts.json`, and `config.json` from the Durable Object when that copy is newer (see
   below). If any of these reads fails, it stops rather than start `ocx` with an empty home or
   older credentials.
3. Starts `ocx`, renews the lease every 30 seconds, and uploads a snapshot every 30 seconds if
   anything changed.
4. On `SIGTERM` (sleep or a new rollout), stops `ocx`, uploads a final snapshot with retries, and
   releases the lease. Cloudflare allows up to 15 minutes between `SIGTERM` and `SIGKILL`, and
   stops the old container before starting the new one.

SQLite databases are copied with `VACUUM INTO`, so a snapshot never holds a half-written database.
Lock databases, the generated management token, and `routing-history.sqlite` are left out; `ocx`
rebuilds that index from `usage.jsonl` the first time it is queried after a boot, as it already did
for a restored copy. Other files are copied as they are; the final snapshot is taken after `ocx` has
exited, so it cannot catch a file mid-write.

Every snapshot carries the whole usage ledger. When a deployment first boots from
`OCX_BOOTSTRAP_CONFIG_JSON`, the seeded config caps the ledger at 32 MiB (`usageLedgerMaxBytes`)
unless the bootstrap config sets its own, which must be at least 1 MiB. A deployment without a
bootstrap config, or one that already has a snapshot, is not changed; set `usageLedgerMaxBytes`
yourself there. Past the cap the oldest rows are dropped. Usage rows written after the last
snapshot are lost if the container dies without `SIGTERM`; a normal stop includes them in the
final snapshot.

Credentials and settings do not wait for a snapshot. Each change to `auth.json`,
`codex-accounts.json`, or `config.json` is also written to the Durable Object, numbered, so a token
rotated or a setting saved seconds before the container stops is not replaced by an older one on the
next boot. `auth.json` changes wait for that write (up to about 10 seconds); the others are written
locally first and sent right after. If the Durable Object cannot be reached, the change is still
saved locally and retried in the background; the snapshot then carries it, and the next boot keeps
whichever copy is newer.

Adding or removing a Codex pool account changes both `config.json` and `codex-accounts.json`,
which are mirrored separately. If one of them could not be sent and the container dies without
`SIGTERM` before the next snapshot, the two can come back out of step: the account is missing from
the list, or listed but asking you to sign in again. Repeat the change to fix it.

If a container dies without `SIGTERM`, other changes since its last snapshot are lost, and the next
container waits up to two minutes for the dead one's lease to expire. A container that loses its
lease stops without uploading, and its late uploads are discarded, so it cannot overwrite newer
state.

Two settings tune this. Neither is secret, but store them with `npx wrangler secret put` like the
others, so that deploying the repository's `wrangler.jsonc` does not reset them:

| Setting | Default | Effect |
|---|---|---|
| `OCX_SNAPSHOT_INTERVAL_SECONDS` | `30` | Upload interval, clamped to 5–60 seconds |
| `OCX_SLEEP_AFTER` | `30m` | How long the container stays up without requests, for example `2h` |

Changing any value the container receives (`OCX_SNAPSHOT_INTERVAL_SECONDS`, `OCX_BOOTSTRAP_CONFIG_JSON`,
the tokens, or a passthrough secret) restarts the container on the next request, as described
under [Operate](#operate).

## Recover a hub that will not start

If the hub answers `503` indefinitely, the saved state may be unusable: for example, the snapshot
object was deleted from R2 while the Durable Object still points at it. The supervisor refuses to
start `ocx` on an empty home in that case, so it cannot overwrite what was saved. To start over
from `OCX_BOOTSTRAP_CONFIG_JSON`, set `OCX_DISCARD_SAVED_STATE` to a new value as shown in
[Change configuration later](#change-configuration-later). Each distinct value is honored once, so
leaving it set does not wipe later boots.

## Security

- The R2 bucket holds `config.json`, OAuth credentials, client API keys, and usage history, and the
  Durable Object's storage holds copies of `config.json` (which can contain provider API keys),
  `auth.json`, and `codex-accounts.json`. Treat access to either, and to the Cloudflare account, as
  access to those credentials.
- The data token and any client keys are the only thing between the internet and your provider
  accounts. Use long random values.
- The Worker keeps `/api/*` and the dashboard closed. To open them, set your own
  `OPENCODEX_ADMIN_AUTH_TOKEN` (different from the data token) and `OCX_EXPOSE_MANAGEMENT_API=1`, as
  in [Dashboard](#dashboard). Anyone with that token can then administer the hub from the internet,
  so use a long random value and rotate it if it leaks; a rotation takes effect within seconds.
- Do not put tokens in `wrangler.jsonc`; `vars` there are stored in plain text.
- The Worker always sends requests to port `10100`; a client cannot reach any other port in the
  container.
- `wrangler.jsonc` keeps Workers Logs on for `console` output but turns invocation logs off, because
  they record request metadata that can include a client's key header. If you turn them back on, treat
  the logs as holding credentials.
- `OCX_PASSTHROUGH_SECRETS` refuses names that control the process rather than carry a secret, such
  as `HOME`, `PATH`, `NODE_OPTIONS`, `LD_PRELOAD`, and the proxy variables. The Worker log names each
  refused entry.
- Remote Workspace pairing limits failed attempts per client IP address. Every request reaches `ocx`
  from the Worker, so on this deployment the limit is shared: one client's failed attempts can lock
  everyone out of pairing until the window passes.
- Snapshot objects are stored under the Durable Object's id in the bucket, and the cleanup at boot
  deletes only inside that prefix, so a bucket shared by mistake does not lose another deployment's
  state. Give each deployment its own bucket anyway.

## Operate

| Task | Command |
|---|---|
| Follow Worker logs | `npx wrangler tail` |
| Update to a new release | `git pull`, then `npx wrangler deploy` |
| Rotate the data token | Repeat the two token lines from [Deploy](#deploy), save the new value, and update your clients |

A running container keeps the secrets it started with. When any secret it receives changes, a
request within seconds of the change stops the container, which saves its state, and starts a new
one with the new values (measured at 2–21 seconds after `wrangler secret put` returned);
requests in between get `503` with `Retry-After`. After a rotation, confirm that a request with the
old token gets `401`.

Rotating `OPENCODEX_API_AUTH_TOKEN` revokes only that token. Client keys stored in `apiKeys`,
including a key's `pendingRotation` value, are part of the saved state in R2 and keep working until
you remove them through the management API or discard the saved state.

For local testing with `npx wrangler dev`, stopping it with Ctrl-C kills the local container without
a final snapshot. The next start waits up to two minutes for the old lease to expire, then restores
the last periodic snapshot. Deployed containers are not affected: Cloudflare sends `SIGTERM` first.

## Limits

- One container serves every request (`max_instances: 1`), because the hub's stores assume a
  single writer.
- Each deployment needs its own Worker `name` and R2 `bucket_name` in `wrangler.jsonc`; two
  deployments with the same names in one account overwrite each other.
- The Worker and container are billed separately from the $5 Workers Paid base; see
  [Containers pricing](https://developers.cloudflare.com/containers/pricing/).
- Cloudflare's Deploy to Cloudflare button does not document support for Containers, so deploy with
  Wrangler as above.

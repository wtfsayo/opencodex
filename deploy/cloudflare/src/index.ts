import { Container, ContainerProxy, getContainer } from "@cloudflare/containers";
import {
  chatAdmitsDataToken, containerEnv, dashboardEnabled, DASHBOARD_BOOTSTRAP_META, DASHBOARD_HTML_HEADERS, edgeDecision, envFingerprint,
  forwardableRequest, isAnonymousHealthCheck, isSupersededBy, servedByHub, type EdgeEnv,
} from "./container-env";
import { type DurableDocument, LeaseState } from "./lease";
import { handleWorkersAi, WORKERS_AI_HOST, type AiRunner } from "./workers-ai";
import { serveNativeChat } from "ocx-worker-native";
import { handleStateRequest } from "./state-routes";

export { ContainerProxy };

export interface Env extends EdgeEnv {
  /** Workers AI binding; the container uses it as an OpenAI-chat provider at http://ai.ocx.internal/v1. */
  AI?: AiRunner;
  /** gui/dist, served by the Worker when dashboardEnabled(); see wrangler.jsonc `assets`. */
  ASSETS?: Fetcher;
  /** version_metadata binding; lets a stale Durable Object notice a newer Worker version. */
  CF_VERSION?: WorkerVersionMetadata;
  HUB: DurableObjectNamespace<OpencodexHub>;
  STATE: R2Bucket;
  OCX_SLEEP_AFTER?: string;
  /** Any new value discards the saved state once; see `applyPendingReset`. */
  OCX_DISCARD_SAVED_STATE?: string;
  /** "1" answers eligible streamed Chat Completions in the Worker; see native/chat.ts. */
  OCX_WORKER_NATIVE?: string;
}

// Must match STATE_ORIGIN in docker/cloudflare-supervisor.ts.
const STATE_HOST = "state.ocx.internal";
const HUB_NAME = "hub";
const STARTED_ENV_KEY = "ocx:started-env";
const HONORED_RESET_KEY = "ocx:honored-reset";
const STOP_WAIT_MS = 5 * 60_000;
// The supervisor closes its 503 placeholder a moment before ocx binds the port.
const HANDOFF_WINDOW_MS = 30_000;
const PROXY_FAILURE = "Error proxying request to container";
const NOT_LISTENING = /not listening/i;
// The library's answer when a connection drops mid-request: the request may already have run.
const DISCONNECTED = "Container suddenly disconnected";
const REPLAYABLE_METHODS = new Set(["GET", "HEAD"]);

export class OpencodexHub extends Container<Env> {
  defaultPort = 10100;
  // The library fetches `http://${pingEndpoint}`, so this is host + path, not a path.
  pingEndpoint = "localhost/healthz";
  sleepAfter = this.env.OCX_SLEEP_AFTER || "30m";
  entrypoint = ["bun", "docker/cloudflare-supervisor.ts"];
  envVars = containerEnv(this.env);

  private readonly leases = new LeaseState(this.ctx.storage);

  private startedAt = 0;
  private resetInFlight: Promise<void> | undefined;

  override async onStart(): Promise<void> {
    this.startedAt = Date.now();
    await this.ctx.storage.put(STARTED_ENV_KEY, await envFingerprint(this.envVars));
  }

  override async fetch(req: Request): Promise<Response> {
    // Rebuilt per request: bindings can change under a live object, and a stale copy would both
    // hide a rotated secret from the check below and start the replacement with the old value.
    this.envVars = containerEnv(this.env);
    await this.applyPendingReset();
    await this.restartIfEnvChanged();
    // This request may itself start the container, which sets startedAt only once it is up, so
    // the window runs from whichever is later: the last start or this request's arrival.
    // startedAt is 0 in a recreated object even when the persisted state says healthy; the
    // container may be stopped and restarted by this very request, so that counts as a new start.
    const windowStart = (await this.getState()).status !== "healthy" || this.startedAt === 0 ? Date.now() : this.startedAt;
    const inWindow = () => Date.now() - Math.max(windowStart, this.startedAt) <= HANDOFF_WINDOW_MS;
    if (!inWindow()) return this.proxy(req);
    // Only inside the handoff window is the request cloned, so a refused connection can be replayed.
    // A refused connection never reached ocx, so any request replays. A dropped one may have run,
    // so only reads replay. Every other failure returns at once.
    for (let attempt = 0; ; attempt++) {
      const response = await this.proxy(req.clone());
      if (response.status !== 500 || attempt >= 10 || !inWindow()) return response;
      const text = await response.text();
      const refused = text.startsWith(PROXY_FAILURE) && NOT_LISTENING.test(text);
      const droppedRead = text.startsWith(DISCONNECTED) && REPLAYABLE_METHODS.has(req.method);
      if (!refused && !droppedRead) return new Response(text, response);
      await new Promise(resolve => setTimeout(resolve, 500));
    }
  }

  // Never super.fetch: it lets a `cf-container-target-port` header pick any port in the container.
  private proxy(req: Request): Promise<Response> {
    return this.containerFetch(req, this.defaultPort);
  }

  // The only way out of a saved state that cannot start (a config that binds loopback, a committed
  // pointer whose object was deleted): the admin API is closed and the bootstrap secret applies only
  // when nothing is saved. Honored once per distinct value, so leaving it set cannot wipe every boot.
  private async applyPendingReset(): Promise<void> {
    // stopAndWait() awaits timers, which reopen the input gate. Without this chain a second request
    // could read the old nonce mid-reset and later discard the replacement container's fresh state.
    while (this.resetInFlight) await this.resetInFlight;
    const nonce = this.env.OCX_DISCARD_SAVED_STATE?.trim();
    if (!nonce) return;
    // Assigned before the first await, so the nonce read is inside the serialized section.
    this.resetInFlight = this.discardSavedStateOnce(nonce).finally(() => { this.resetInFlight = undefined; });
    await this.resetInFlight;
  }

  private async discardSavedStateOnce(nonce: string): Promise<void> {
    if ((await this.ctx.storage.get<string>(HONORED_RESET_KEY)) === nonce) return;
    console.log("OCX_DISCARD_SAVED_STATE changed; discarding the saved state.");
    // Stop first: a running container uploads a final snapshot on the way out, which would
    // otherwise re-commit the state being discarded.
    await this.stopAndWait();
    const discarded = await this.leases.discardSnapshot();
    if (discarded) await this.env.STATE.delete(discarded);
    await this.ctx.storage.put(HONORED_RESET_KEY, nonce);
  }

  // A running container keeps the environment it started with, and every request renews its idle
  // timer, so a rotated data token would otherwise stay valid for as long as the leaked one is used.
  private async restartIfEnvChanged(): Promise<void> {
    if ((await this.getState()).status !== "healthy") return;
    const started = await this.ctx.storage.get<string>(STARTED_ENV_KEY);
    if (!started || started === (await envFingerprint(this.envVars))) return;
    console.log("Container secrets changed; restarting the container.");
    await this.stopAndWait();
  }

  private async stopAndWait(): Promise<void> {
    if (!["running", "healthy"].includes((await this.getState()).status)) return;
    const stoppedAt = Date.now();
    await this.stop("SIGTERM");
    // Done once the state moves after our stop: the old process exited, or another request
    // already started its replacement.
    const deadline = stoppedAt + STOP_WAIT_MS;
    while (Date.now() < deadline) {
      const state = await this.getState();
      if (state.lastChange >= stoppedAt || !["running", "healthy"].includes(state.status)) break;
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
  }

  /** The container's own /healthz if it is already running; never starts it. */
  async healthIfRunning(): Promise<Response> {
    if ((await this.getState()).status !== "healthy") {
      return Response.json({ status: "sleeping" }, { status: 503, headers: { "retry-after": "30", "cache-control": "no-store" } });
    }
    return this.proxy(new Request("http://container/healthz"));
  }

  /** Resets this object when the calling Worker is newer, so the next instance sees current secrets. */
  assertCurrentVersion(workerVersionTimestamp: string | undefined): void {
    if (isSupersededBy(workerVersionTimestamp, this.env.CF_VERSION?.timestamp)) {
      this.ctx.abort("superseded by a newer Worker version");
    }
  }

  acquireLease(bootId: string) { return this.leases.acquireLease(bootId); }
  renewLease(bootId: string) { return this.leases.renewLease(bootId); }
  holdsLease(bootId: string) { return this.leases.holdsLease(bootId); }
  releaseLease(bootId: string) { return this.leases.releaseLease(bootId); }
  currentSnapshot() { return this.leases.currentSnapshot(); }
  commitSnapshot(bootId: string, key: string) { return this.leases.commitSnapshot(bootId, key); }
  readDocument(name: DurableDocument) { return this.leases.readDocument(name); }
  commitDocument(bootId: string, name: DurableDocument, body: string, seq: number) { return this.leases.commitDocument(bootId, name, body, seq); }
}

async function handleState(req: Request, env: Env): Promise<Response> {
  // Snapshot keys live under this object's id, so a bucket shared with another deployment is safe.
  const namespace = env.HUB.idFromName(HUB_NAME).toString();
  return handleStateRequest(req, getContainer(env.HUB, HUB_NAME), {
    get: async key => (await env.STATE.get(key))?.body ?? null,
    put: async (key, body, length) => { await env.STATE.put(key, body.pipeThrough(new FixedLengthStream(length))); },
    delete: key => env.STATE.delete(key),
    list: async (prefix, limit) => {
      const keys: string[] = [];
      let cursor: string | undefined;
      do {
        const page = await env.STATE.list({ prefix, cursor, limit: Math.min(1000, limit - keys.length) });
        keys.push(...page.objects.map(object => object.key));
        cursor = page.truncated ? page.cursor : undefined;
      } while (cursor && keys.length < limit);
      return keys;
    },
  }, namespace);
}

OpencodexHub.outboundByHost = {
  [STATE_HOST]: handleState,
  [WORKERS_AI_HOST]: (req: Request, env: Env) => handleWorkersAi(req, env.AI),
};

async function serveDashboard(req: Request, assets: Fetcher): Promise<Response> {
  const response = await assets.fetch(req);
  if (!(response.headers.get("content-type") ?? "").startsWith("text/html")) return response;
  const page = new HTMLRewriter()
    .on("head", { element(head) { head.append(DASHBOARD_BOOTSTRAP_META, { html: true }); } })
    .transform(response);
  const headers = new Headers(page.headers);
  for (const [name, value] of Object.entries(DASHBOARD_HTML_HEADERS)) headers.set(name, value);
  return new Response(page.body, { status: page.status, headers });
}

// Larger bodies stream to the container untouched: reading them here costs Worker memory (128 MB)
// and a text-only turn this path would serve is far smaller.
const NATIVE_MAX_BODY_BYTES = 4 * 1024 * 1024;

/**
 * The Worker-native path runs only where the edge has matched the data token exactly: with
 * OCX_EDGE_KEY_CHECK=presence the key is verified by ocx, which this path would skip. It also
 * applies ocx's own header rule for chat (chatAdmitsDataToken). Returns the response, or the
 * request to forward when the body was read and declined, or null when the request is untouched.
 */
async function tryWorkerNative(req: Request, env: Env): Promise<Response | { forward: Request } | null> {
  if (env.OCX_WORKER_NATIVE?.trim() !== "1" || env.OCX_EDGE_KEY_CHECK?.trim() === "presence") return null;
  if (req.method !== "POST" || new URL(req.url).pathname !== "/v1/chat/completions") return null;
  // ocx decompresses gzip and zstd bodies; this path would have to as well, so leave them to it.
  if (req.headers.has("content-encoding")) return null;
  const length = Number(req.headers.get("content-length"));
  if (!Number.isSafeInteger(length) || length <= 0 || length > NATIVE_MAX_BODY_BYTES) return null;
  if (!(await chatAdmitsDataToken(req, env))) return null;
  const bodyBytes = await req.arrayBuffer();
  const hub = getContainer(env.HUB, HUB_NAME);
  try {
    const served = await serveNativeChat(new TextDecoder().decode(bodyBytes), req.signal, {
      readConfig: async () => (await hub.readDocument("config"))?.body,
      localHosts: { [WORKERS_AI_HOST]: request => handleWorkersAi(request, env.AI) },
      fetch: request => fetch(request),
    });
    if (served) return served;
  } catch (error) {
    console.error(`Worker-native chat declined after an error: ${error instanceof Error ? error.message : String(error)}`);
  }
  // Nobody is waiting for an answer, so do not wake the container to produce one.
  if (req.signal.aborted) return new Response(null, { status: 499 });
  return { forward: new Request(req, { body: bodyBytes }) };
}

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    // The dashboard's static files come from the Worker and never start the container; the admin
    // token it asks for is checked at the edge on /api/* and again by ocx.
    if (env.ASSETS && dashboardEnabled(env) && !servedByHub(new URL(req.url).pathname)) {
      return serveDashboard(req, env.ASSETS);
    }
    if (env.OPENCODEX_API_AUTH_TOKEN && isAnonymousHealthCheck(req)) {
      return getContainer(env.HUB, HUB_NAME).healthIfRunning();
    }
    const decision = await edgeDecision(req, env);
    if (!decision.forward) {
      if (decision.status === 204) return new Response(null, { status: 204 });
      return Response.json({ error: { message: decision.message, type: "invalid_request_error" } }, { status: decision.status });
    }
    const native = await tryWorkerNative(req, env);
    if (native instanceof Response) return native;
    if (native) req = native.forward;
    // Only this body-free call is retried: an aborted stale object rejects it until the fresh
    // instance is up. The request itself is sent once, so a body is never replayed.
    let current = false;
    for (let attempt = 0; attempt < 5 && !current; attempt++) {
      try {
        await getContainer(env.HUB, HUB_NAME).assertCurrentVersion(env.CF_VERSION?.timestamp);
        current = true;
      } catch {
        await new Promise(resolve => setTimeout(resolve, 100 * (attempt + 1)));
      }
    }
    try {
      if (!current) throw new Error("hub object did not come back after a version reset");
      return await getContainer(env.HUB, HUB_NAME).fetch(forwardableRequest(req));
    } catch (error) {
      // A reset or restart racing this request: an answer the client can retry, not a bare 1101.
      console.error(`Hub request failed: ${error instanceof Error ? error.message : String(error)}`);
      return Response.json(
        { error: { message: "opencodex is restarting; retry shortly.", type: "server_error" } },
        { status: 503, headers: { "retry-after": "5" } },
      );
    }
  },
} satisfies ExportedHandler<Env>;

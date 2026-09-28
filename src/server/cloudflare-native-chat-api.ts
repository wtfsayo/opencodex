// The contract between the Cloudflare Worker (deploy/cloudflare) and the Worker-native Chat
// Completions path in cloudflare-native-chat.ts. Import-free: the Worker package typechecks it
// against Workers types, where the ocx modules behind the implementation do not typecheck.

export type NativeChatDeps = {
  /** The hub's config.json: the Durable Object's copy, or the bootstrap config before one exists. */
  readConfig(): Promise<string | undefined>;
  /** The Durable Object's copy of auth.json, read only for a turn to an OAuth provider. */
  readAuth?(): Promise<string | undefined>;
  /** Upstream hosts answered inside the Worker (the Workers AI binding), by host name. */
  localHosts?: Record<string, (request: Request) => Promise<Response>>;
  fetch(request: Request): Promise<Response>;
  /** The environment ocx would run with, for `${NAME}` key references; see containerEnv. */
  secrets?: Readonly<Record<string, string>>;
  /**
   * The Worker's per-session <skills_instructions> freeze. `read` returns the block a session is
   * frozen to; `commit` freezes it to one once the turn has been sent (first block wins). Scopes are
   * hex digests; `principal` is folded into them and never stored.
   */
  skills?: { read(scope: string): Promise<string | undefined>; commit(scope: string, block: string): void; principal: string };
  /**
   * ocx's reasoning-effort caches as it last published them (reasoning-metadata.ts): each the JSON
   * of its file, "null" when ocx has none, absent when never published.
   */
  reasoningMetadata?(): Promise<{ snapshot?: string; support?: string }>;
  /**
   * The Claude Code fingerprint headers naming ocx's runtime, as this deployment's ocx published
   * them (worker-native-state.ts); undefined until it has.
   */
  clientRuntime?(): Promise<Record<string, string> | undefined>;
  /** Whether a value is one of the hub's own admission or admin keys, which ocx never forwards upstream. */
  isAdmissionSecret?(value: string): Promise<boolean>;
  /** The Durable Object's copy of codex-accounts.json, read only for a turn to the native OpenAI provider. */
  readCodexAccounts?(): Promise<string | undefined>;
  /** What ocx's process holds that a native OpenAI turn depends on, as published for this deployment. */
  nativeOpenAiFacts?(): Promise<NativeOpenAiFacts | undefined>;
  /** Dials the ChatGPT backend's WebSocket with these upgrade headers; undefined where none can be. */
  openUpstreamSocket?(url: string, headers: Record<string, string>): WebSocket;
  /** Called once per served turn, after its last byte; the Worker queues it for ocx's usage log. */
  recordUsage?(row: WorkerUsageRow): void;
  /** Why a request went to the container. Reasons name config keys and fields, never values. */
  onDecline?(reason: string): void;
};

/**
 * ocx's own state that decides a ChatGPT passthrough turn (server/worker-native-state.ts publishes
 * it). Newer versions from one process replace older ones.
 */
export type NativeOpenAiFacts = {
  version: number;
  /**
   * main-account-cache.ts's identity key (a sha256 of the account id) of the main login ocx has
   * observed, or null: a caller holding that account gets ocx's hard lock and cooldowns.
   */
  mainAccountIdentityKey: string | null;
  /** Codex accounts in ocx's store: pool selection then decides whose login a turn uses. */
  codexAccountsStored: boolean;
  /**
   * A main Codex login in ocx's CODEX_HOME (auth-collision.ts's getMainChatgptAccountId): ocx's
   * web-search sidecar then searches with it, where without one it drops a hosted web_search tool.
   */
  mainCodexLoginPresent: boolean;
  nativeMainTrafficBlocked: boolean;
  contextRelayActive: boolean;
  /** "proxied" when an egress proxy carries ocx's upstream traffic, which the Worker cannot use. */
  upstreamTransport: "websocket" | "sse" | "proxied";
  /** input-admission.ts's ceiling for each native model id ocx knows, null where it has none. */
  inputCeilings: Record<string, number | null>;
  /**
   * vision/plan.ts's requiresVisionPreprocessing for each configured `provider/model`: true where ocx
   * describes or strips a turn's images before sending, from catalogs only it reads.
   */
  visionPreprocessed: Record<string, boolean>;
};

/** Serves the turn, or returns null to hand the request (with `bodyText`) to the container. */
export type ServeNativeChat = (bodyText: string, headers: Headers, signal: AbortSignal, deps: NativeChatDeps) => Promise<Response | null>;

/**
 * A usage-log row for a turn the Worker served: the fields of ocx's PersistedUsageEntry
 * (src/usage/log.ts) this path knows. ocx appends it through appendUsageEntry when it next runs.
 */
export type WorkerUsageRow = {
  requestId: string;
  timestamp: number;
  provider: string;
  model: string;
  requestedModel: string;
  /** `k<hex32>`, ocx's digest of the provider and its configured key reference. */
  accountLogLabel?: string;
  requestedEffort?: string;
  resolvedModel?: string;
  /** The id sent upstream, when the client is answered with another (an Anthropic route's selector). */
  wireModel?: string;
  /** ocx's hashed conversation id for the turn, as request-log-conversation.ts derives it. */
  conversationId?: string;
  surface?: "claude";
  inboundProtocol: "chat" | "responses" | "messages";
  admissionKind: "environment";
  status: number;
  durationMs: number;
  firstOutputMs?: number;
  usageStatus: "reported" | "unreported";
  usage?: { inputTokens: number; outputTokens: number } & Record<string, unknown>;
  totalTokens?: number;
  /**
   * What ocx reserves for this send in its spend ledger before the answer (request-spend.ts): the
   * input estimate its path makes and the output ceiling the caller set. Booked, not logged.
   */
  spendInputTokens?: number;
  spendOutputCeilingTokens?: number;
};

/** The replay key for a GET /v1/models request, or undefined when its answer cannot be replayed. */
export type ModelListReplayKey = (url: URL, headers: Headers) => Promise<string | undefined>;

/** A socket from the Worker to ocx's own Responses WebSocket, opened on first use. */
export interface NativeWsContainer {
  send(text: string): void;
  close(): void;
}

/** What a Worker-held Responses WebSocket session needs from the Worker. */
export interface NativeWsLink {
  /** Sends a text frame to the client. */
  send(text: string): void;
  close(code: number, reason: string): void;
  /** Opens a socket to ocx with the client's upgrade request; its frames and close come back through the session. */
  openContainer(): NativeWsContainer;
}

export interface NativeWsSession {
  receive(data: string | ArrayBuffer): void;
  fromContainer(from: NativeWsContainer, text: string): void;
  containerClosed(from: NativeWsContainer, code: number, reason: string): void;
  /** The client went away. */
  closed(): void;
}

export type CreateNativeWsSession = (link: NativeWsLink, upgradeHeaders: Headers, deps: NativeChatDeps) => NativeWsSession;

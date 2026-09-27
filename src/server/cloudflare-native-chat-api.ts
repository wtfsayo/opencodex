// The contract between the Cloudflare Worker (deploy/cloudflare) and the Worker-native Chat
// Completions path in cloudflare-native-chat.ts. Import-free: the Worker package typechecks it
// against Workers types, where the ocx modules behind the implementation do not typecheck.

export type NativeChatDeps = {
  /** The hub's config.json: the Durable Object's copy, or the bootstrap config before one exists. */
  readConfig(): Promise<string | undefined>;
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
  /** Called once per served turn, after its last byte; the Worker queues it for ocx's usage log. */
  recordUsage?(row: WorkerUsageRow): void;
  /** Why a request went to the container. Reasons name config keys and fields, never values. */
  onDecline?(reason: string): void;
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
  inboundProtocol: "chat" | "responses";
  admissionKind: "environment";
  status: number;
  durationMs: number;
  firstOutputMs?: number;
  usageStatus: "reported" | "unreported";
  usage?: { inputTokens: number; outputTokens: number } & Record<string, unknown>;
  totalTokens?: number;
};


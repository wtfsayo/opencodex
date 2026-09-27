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
  /** Why a request went to the container. Reasons name config keys and fields, never values. */
  onDecline?(reason: string): void;
};

/** Serves the turn, or returns null to hand the request (with `bodyText`) to the container. */
export type ServeNativeChat = (bodyText: string, headers: Headers, signal: AbortSignal, deps: NativeChatDeps) => Promise<Response | null>;

// The contract between the Cloudflare Worker (deploy/cloudflare) and the Worker-native Chat
// Completions path in cloudflare-native-chat.ts. Import-free: the Worker package typechecks it
// against Workers types, where the ocx modules behind the implementation do not typecheck.

export type NativeChatDeps = {
  /** The Durable Object's mirror of the hub's config.json, or undefined when it has none. */
  readConfig(): Promise<string | undefined>;
  /** Upstream hosts answered inside the Worker (the Workers AI binding), by host name. */
  localHosts?: Record<string, (request: Request) => Promise<Response>>;
  fetch(request: Request): Promise<Response>;
  /** Why a request went to the container. Reasons name config keys and fields, never values. */
  onDecline?(reason: string): void;
};

/** Serves the turn, or returns null to hand the request (with `bodyText`) to the container. */
export type ServeNativeChat = (bodyText: string, headers: Headers, signal: AbortSignal, deps: NativeChatDeps) => Promise<Response | null>;

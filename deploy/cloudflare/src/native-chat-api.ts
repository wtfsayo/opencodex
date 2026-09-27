// The Worker-native Chat Completions path lives in src/server/cloudflare-native-chat.ts so that it is
// typechecked with the ocx modules it reuses, which do not typecheck against Workers types. This
// package sees it only through this declaration; wrangler.jsonc aliases "ocx-worker-native" to the
// real module, which is declared as a ServeNativeChat, so the two cannot drift apart.
import type { ServeNativeChat } from "../../../src/server/cloudflare-native-chat-api";

export type { NativeChatDeps, ServeNativeChat } from "../../../src/server/cloudflare-native-chat-api";
export declare const serveNativeChat: ServeNativeChat;

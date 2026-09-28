// The Worker-native paths live in src/server/cloudflare-native.ts so that it is
// typechecked with the ocx modules it reuses, which do not typecheck against Workers types. This
// package sees it only through this declaration; wrangler.jsonc aliases "ocx-worker-native" to the
// real module, which is declared as a ServeNativeChat, so the two cannot drift apart.
import type { ServeNativeChat } from "../../../src/server/cloudflare-native-chat-api";

export type { NativeChatDeps, ServeNativeChat } from "../../../src/server/cloudflare-native-chat-api";
export declare const serveNativeChat: ServeNativeChat;
export declare const serveNativeResponses: ServeNativeChat;
export declare const serveNativeMessages: ServeNativeChat;

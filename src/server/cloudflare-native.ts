// The Cloudflare Worker's entry into ocx code (deploy/cloudflare aliases "ocx-worker-native" here).
// Everything reachable from this file runs in the Workers runtime; see
// tests/service/cloudflare-worker-native.test.ts for the import rules that keep it so.
import type { ServeNativeChat } from "./cloudflare-native-chat-api";
import { serveNativeChat as chat } from "./cloudflare-native-chat";
import { serveNativeResponses as responses } from "./cloudflare-native-responses";
import { serveNativeMessages as messages } from "./cloudflare-native-messages";

export const serveNativeChat: ServeNativeChat = chat;
export const serveNativeResponses: ServeNativeChat = responses;
export const serveNativeMessages: ServeNativeChat = messages;

// The Cloudflare Worker's entry into ocx code (deploy/cloudflare aliases "ocx-worker-native" here).
// Everything reachable from this file runs in the Workers runtime; see
// tests/service/cloudflare-worker-native.test.ts for the import rules that keep it so.
import type { CreateNativeWsSession, ModelListReplayKey, ServeNativeChat } from "./cloudflare-native-chat-api";
import { nativeConfigAdmitted as configAdmitted, serveNativeChat as chat } from "./cloudflare-native-chat";
import { serveNativeResponses as responses } from "./cloudflare-native-responses";
import { serveNativeMessages as messages } from "./cloudflare-native-messages";
import { modelListReplayKey as replayKey } from "./model-list-replay-key";
import { createNativeWsSession as wsSession } from "./cloudflare-native-ws";

export const serveNativeChat: ServeNativeChat = chat;
export const serveNativeResponses: ServeNativeChat = responses;
export const serveNativeMessages: ServeNativeChat = messages;
export const modelListReplayKey: ModelListReplayKey = replayKey;
export const createNativeWsSession: CreateNativeWsSession = wsSession;
export const nativeConfigAdmitted: (config: unknown) => boolean = configAdmitted;

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { existsSync, mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DURABLE_STATE_BOOT_ID_ENV, setDurableMirrorTransportForTests } from "../../src/lib/durable-mirror";
import { drainWorkerUsageInbox } from "../../src/usage/worker-usage-inbox";
import { usageLogPath } from "../../src/usage/log";
import { removeTreeWithRetry } from "../helpers/remove-tree";

let home: string;
let previousHome: string | undefined;
let previousBootId: string | undefined;

const row = (n: number) => ({
  requestId: `00000000-0000-4000-8000-00000000000${n}`, timestamp: 1_790_000_000_000 + n, provider: "workers-ai", model: "meta/llama",
  requestedModel: "workers-ai/meta/llama", inboundProtocol: "chat", admissionKind: "environment", status: 200, durationMs: 400, usageStatus: "unreported",
});

describe("Worker usage inbox", () => {
  beforeEach(() => {
    previousHome = process.env.OPENCODEX_HOME;
    previousBootId = process.env[DURABLE_STATE_BOOT_ID_ENV];
    home = mkdtempSync(join(tmpdir(), "ocx-usage-inbox-"));
    process.env.OPENCODEX_HOME = home;
  });

  afterEach(() => {
    setDurableMirrorTransportForTests(null);
    if (previousHome === undefined) delete process.env.OPENCODEX_HOME;
    else process.env.OPENCODEX_HOME = previousHome;
    if (previousBootId === undefined) delete process.env[DURABLE_STATE_BOOT_ID_ENV];
    else process.env[DURABLE_STATE_BOOT_ID_ENV] = previousBootId;
    removeTreeWithRetry(home);
  });

  test("without a boot id nothing is fetched", async () => {
    delete process.env[DURABLE_STATE_BOOT_ID_ENV];
    let calls = 0;
    setDurableMirrorTransportForTests({ fetch: async () => { calls++; return new Response(null, { status: 500 }); } });
    expect(await drainWorkerUsageInbox()).toBe(0);
    expect(calls).toBe(0);
  });

  test("appends queued rows to usage.jsonl, skips malformed ones, and acknowledges the batch", async () => {
    process.env[DURABLE_STATE_BOOT_ID_ENV] = "c".repeat(32);
    const acks: unknown[] = [];
    let served = false;
    setDurableMirrorTransportForTests({
      origin: "http://state.test",
      fetch: async (url, init) => {
        if (url.endsWith("/usage-inbox/ack")) {
          acks.push(JSON.parse(String(init.body)));
          return new Response(null, { status: 204 });
        }
        if (served) return Response.json({ rows: [] });
        served = true;
        return Response.json({ rows: [{ seq: 1, row: row(1) }, { seq: 2, row: { requestId: "x" } }, { seq: 3, row: row(3) }] });
      },
    });
    expect(await drainWorkerUsageInbox()).toBe(2);
    expect(acks).toEqual([{ seqs: [1, 2, 3] }]);
    const path = usageLogPath();
    expect(existsSync(path)).toBe(true);
    const lines = readFileSync(path, "utf8").trim().split("\n").map(line => JSON.parse(line));
    expect(lines.map(line => line.requestId)).toEqual([row(1).requestId, row(3).requestId]);
    expect(lines[0]).toMatchObject({ provider: "workers-ai", status: 200, inboundProtocol: "chat" });
  });

  test("keeps the surface, key label, conversation, resolved model and effort only in the shapes ocx writes", async () => {
    process.env[DURABLE_STATE_BOOT_ID_ENV] = "c".repeat(32);
    const good = {
      ...row(1), surface: "claude", accountLogLabel: `k${"a".repeat(32)}`, conversationId: "b".repeat(32),
      resolvedModel: "meta/llama", requestedEffort: "high",
    };
    const bad = {
      ...row(2), surface: "mystery", accountLogLabel: "sk-live-looking", conversationId: "user@example.com",
      resolvedModel: "x".repeat(201), requestedEffort: 3, extra: "dropped",
    };
    let served = false;
    setDurableMirrorTransportForTests({
      origin: "http://state.test",
      fetch: async url => {
        if (url.endsWith("/usage-inbox/ack")) return new Response(null, { status: 204 });
        if (served) return Response.json({ rows: [] });
        served = true;
        return Response.json({ rows: [{ seq: 1, row: good }, { seq: 2, row: bad }] });
      },
    });
    expect(await drainWorkerUsageInbox()).toBe(2);
    const [kept, cleaned] = readFileSync(usageLogPath(), "utf8").trim().split("\n").map(line => JSON.parse(line));
    expect(kept).toMatchObject({ surface: "claude", accountLogLabel: good.accountLogLabel, conversationId: good.conversationId, resolvedModel: "meta/llama", requestedEffort: "high" });
    for (const field of ["surface", "accountLogLabel", "conversationId", "resolvedModel", "requestedEffort", "extra"]) expect(cleaned[field]).toBeUndefined();
  });
});

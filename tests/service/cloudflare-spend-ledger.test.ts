import { describe, expect, test } from "bun:test";
import { DurableSpendJournal, DurableSpendLedger, spendLedgerSalt, type SpendLedgerSqlStorage } from "../../deploy/cloudflare/src/spend-ledger";
import { createSpendReservationLedger, spendPolicyFromConfig, spendCeilingsConfigured } from "../../src/lib/spend-reservation-core";
import { serveNativeChat } from "../../src/server/cloudflare-native-chat";
import { workerSpendTurn } from "../../src/server/cloudflare-native-spend";
import { WORKERS_AI_HOST, handleWorkersAi } from "../../deploy/cloudflare/src/workers-ai";
import type { NativeChatDeps, WorkerSpendReserveRequest } from "../../src/server/cloudflare-native-chat-api";

// An in-memory stand-in for ctx.storage.sql: the same ordered, deduplicating row semantics the
// DurableSpendJournal's queries rely on, without the runtime. Only the grammar the journal
// issues is parsed; anything else throws so a schema drift fails loudly here.
function memorySql(): SpendLedgerSqlStorage {
  const journal: { seq: number; line: string }[] = [];
  const meta = new Map<string, string>();
  let nextSeq = 1;
  return {
    exec<T = Record<string, unknown>>(query: string, ...bindings: unknown[]) {
      const q = query.replace(/\s+/g, " ").trim();
      let rows: unknown[] = [];
      if (q.startsWith("CREATE TABLE")) {
        // no-op
      } else if (q === "INSERT INTO ocx_spend_journal (line) SELECT ? WHERE NOT EXISTS (SELECT 1 FROM ocx_spend_journal WHERE line = ?)") {
        const line = String(bindings[0]);
        if (!journal.some(row => row.line === line)) journal.push({ seq: nextSeq++, line });
      } else if (q === "INSERT INTO ocx_spend_journal (line) VALUES (?)") {
        journal.push({ seq: nextSeq++, line: String(bindings[0]) });
      } else if (q === "SELECT line FROM ocx_spend_journal ORDER BY seq") {
        rows = journal.map(row => ({ line: row.line }));
      } else if (q === "SELECT seq, line FROM ocx_spend_journal ORDER BY seq") {
        rows = journal.map(row => ({ seq: row.seq, line: row.line }));
      } else if (q === "SELECT seq FROM ocx_spend_journal WHERE line = ? ORDER BY seq DESC LIMIT 1") {
        const found = journal.filter(row => row.line === bindings[0]).at(-1);
        rows = found ? [{ seq: found.seq }] : [];
      } else if (q === "DELETE FROM ocx_spend_journal") {
        journal.length = 0;
      } else if (q === "SELECT value FROM ocx_spend_meta WHERE key = ?") {
        const value = meta.get(String(bindings[0]));
        rows = value === undefined ? [] : [{ value }];
      } else if (q === "INSERT INTO ocx_spend_meta (key, value) VALUES (?, ?)") {
        meta.set(String(bindings[0]), String(bindings[1]));
      } else {
        throw new Error(`unhandled sql: ${q}`);
      }
      return { toArray: () => rows as T[] };
    },
    transactionSync<T>(fn: () => T): T {
      const snapshot = journal.map(row => ({ ...row }));
      try {
        return fn();
      } catch (error) {
        journal.length = 0;
        journal.push(...snapshot);
        throw error;
      }
    },
  };
}

const configWithSpend = (spend: unknown) => JSON.stringify({ providers: {}, spend });

describe("DurableSpendJournal over DO SQL", () => {
  test("reads rows in write order, dedupes a retried line, and rewrites atomically", () => {
    const sql = memorySql();
    const salt = spendLedgerSalt(sql);
    expect(salt).toMatch(/^[0-9a-f]{64}$/);
    const journal = new DurableSpendJournal(sql, salt);
    journal.append(JSON.stringify({ v: 1, kind: "reserve", send: "a1", targets: [], tokens: 5, at: 1 }));
    journal.append(JSON.stringify({ v: 1, kind: "dispatch", send: "a1", at: 2 }));
    journal.append(JSON.stringify({ v: 1, kind: "dispatch", send: "a1", at: 2 }));
    expect(journal.read()).toHaveLength(2);
    journal.rewrite([JSON.stringify({ v: 1, kind: "checkpoint", at: 9, scopes: [], sends: [] })]);
    expect(journal.read()).toHaveLength(1);
    expect(JSON.parse(journal.read()[0]!)).toMatchObject({ kind: "checkpoint" });
  });

  test("reserves, dispatches and settles against the parsed config policy, then replays", async () => {
    const sql = memorySql();
    const spend = { pool: { maxTokens: 100 } };
    const hub = new DurableSpendLedger(sql, async () => configWithSpend(spend));
    const decision = await hub.spendReserve({
      sendId: "s1",
      scopes: { poolId: "p" },
      inputTokens: 30,
      outputCeilingTokens: 40,
    });
    expect(decision).toMatchObject({ reserved: true, sendId: "s1", tokens: 70 });
    expect(await hub.spendMarkDispatched("s1")).toBe(true);
    expect(await hub.spendKnows("s1")).toBe(true);
    expect(await hub.spendSettle("s1", { inputTokens: 12, outputTokens: 8 })).toBe(true);
    expect(await hub.spendSettle("s1", { inputTokens: 12, outputTokens: 8 })).toBe(false);
    const snapshot = await hub.spendSnapshot("pool", "p");
    expect(snapshot).toMatchObject({ settled: 20, reserved: 0, unresolved: 0, exhausted: false });

    // A reconstructed ledger over the same storage replays the journal: the settled figure
    // survives the restart the DO simulates by hibernation.
    const restarted = new DurableSpendLedger(sql, async () => configWithSpend(spend));
    expect(await restarted.spendSnapshot("pool", "p")).toMatchObject({ settled: 20 });
    expect(await restarted.spendKnows("s1")).toBe(true);
  });

  test("refuses the send that would cross a ceiling, and the next one is refused too", async () => {
    const sql = memorySql();
    const hub = new DurableSpendLedger(sql, async () => configWithSpend({ pool: { maxTokens: 100 } }));
    const first = await hub.spendReserve({ sendId: "s1", scopes: { poolId: "p" }, inputTokens: 60, outputCeilingTokens: 30 });
    expect(first).toMatchObject({ reserved: true, tokens: 90 });
    const denied = await hub.spendReserve({ sendId: "s2", scopes: { poolId: "p" }, inputTokens: 60, outputCeilingTokens: 30 });
    expect(denied).toMatchObject({
      reserved: false,
      denial: { reason: "spend-limit-exceeded", scope: "pool", limit: 100, projected: 180 },
    });
    // The denied send never books: a fresh look at the scope shows only the 90 it holds.
    expect(await hub.spendSnapshot("pool", "p")).toMatchObject({ reserved: 90 });
  });

  test("a lost usage frame keeps the reservation as unresolved spend", async () => {
    const sql = memorySql();
    const hub = new DurableSpendLedger(sql, async () => configWithSpend({ pool: { maxTokens: 100 } }));
    await hub.spendReserve({ sendId: "s1", scopes: { poolId: "p" }, inputTokens: 60, outputCeilingTokens: 30 });
    expect(await hub.spendMarkLost("s1")).toBe(true);
    // The full reservation is unresolved, so the next 20-token send is refused at 110.
    const denied = await hub.spendReserve({ sendId: "s2", scopes: { poolId: "p" }, inputTokens: 10, outputCeilingTokens: 10 });
    expect(denied).toMatchObject({ reserved: false, denial: { reason: "spend-limit-exceeded", projected: 110 } });
  });

  test("reconfigure follows the stored config between calls", async () => {
    const sql = memorySql();
    let spend: unknown = { pool: { maxTokens: 100 } };
    const hub = new DurableSpendLedger(sql, async () => configWithSpend(spend));
    expect((await hub.spendReserve({ sendId: "s1", scopes: { poolId: "p" }, inputTokens: 60, outputCeilingTokens: 60 })).reserved).toBe(false);
    spend = { pool: { maxTokens: 1000 } };
    expect((await hub.spendReserve({ sendId: "s1", scopes: { poolId: "p" }, inputTokens: 60, outputCeilingTokens: 60 })).reserved).toBe(true);
    // A ceiling is a ceiling only while configured; without a spend section the ledger observes.
    spend = {};
    expect((await hub.spendReserve({ sendId: "s2", scopes: { poolId: "p" }, inputTokens: 9999, outputCeilingTokens: 0 })).reserved).toBe(true);
  });

  test("the container's pushed records move the books and dedupe on retry", async () => {
    const sql = memorySql();
    const hub = new DurableSpendLedger(sql, async () => configWithSpend({ pool: { maxTokens: 100 } }));
    const reserve = JSON.stringify({
      v: 1, kind: "reserve", send: "c1", at: 1,
      targets: [{ scope: "pool", alias: "sha256" }],
      tokens: 90,
    });
    // The line carries the DO's alias, not the raw pool id: this drives the journal-append
    // path alone, so the alias is supplied directly.
    const seq = hub.appendJournalRecord(reserve);
    expect(typeof seq).toBe("number");
    expect(hub.appendJournalRecord(reserve)).toBe(seq);
    expect(hub.appendJournalRecord("not json")).toBeNull();
    // Importing c1 built the ledger, whose replay met an open reservation with no live writer
    // inside this activation: it is reconciled to unresolved spend, so the journal holds the
    // pushed reserve and the reconciliation's lost record.
    expect(hub.journalSnapshot().entries).toHaveLength(2);
    await hub.spendReserve({ sendId: "w1", scopes: {}, inputTokens: 1, outputCeilingTokens: 0 });
    expect(hub.journalSnapshot().entries).toHaveLength(3);
  });
});

describe("Worker-native spend admission", () => {
  const spendConfig = JSON.stringify({ providers: { p: { adapter: "openai-chat", baseUrl: "https://api.example.test/v1", apiKey: "sk-literal", models: ["m-1"] } }, spend: { pool: { maxTokens: 100 } } });
  const body = JSON.stringify({ model: "p/m-1", stream: false, messages: [{ role: "user", content: "hi" }] });

  test("declines when the config carries a ceiling but the hub cannot reserve", async () => {
    const reasons: string[] = [];
    const response = await serveNativeChat(body, new Headers(), new AbortController().signal, {
      readConfig: async () => spendConfig,
      fetch: async () => { throw new Error("must not be sent"); },
      onDecline: reason => reasons.push(reason),
    });
    expect(response).toBeNull();
    expect(reasons).toContain("spend-ledger-unavailable");
  });

  test("answers a denied reservation with the workflow refusal's 429 shape", async () => {
    const requests: WorkerSpendReserveRequest[] = [];
    const response = await serveNativeChat(body, new Headers(), new AbortController().signal, {
      readConfig: async () => spendConfig,
      fetch: async () => { throw new Error("must not be sent"); },
      spendReserve: async request => {
        requests.push(request);
        return { reserved: false, denial: { reason: "spend-limit-exceeded", scope: "pool", scopeId: "p", limit: 100, projected: 120 } };
      },
      spendAbandon: () => {},
      spendMarkDispatched: () => {},
      spendSettle: () => {},
      spendMarkLost: () => {},
    });
    expect(response?.status).toBe(429);
    expect(response?.headers.get("x-opencodex-local-refusal")).toBe("workflow_spend_exhausted");
    expect(response?.headers.get("Access-Control-Expose-Headers")).toBe("x-opencodex-local-refusal");
    const payload = await response!.json() as { error: { type: string; code: string; message: string } };
    expect(payload.error).toMatchObject({ type: "rate_limit_error", code: "rate_limit_exceeded" });
    expect(payload.error.message).toContain("provider pool");
    expect(payload.error.message).toContain("100");
    expect(requests).toHaveLength(1);
    expect(requests[0]!.scopes.poolId).toBe("p");
  });

  test("reserves before the send and settles with the terminal usage", async () => {
    const settled: { sendId: string; input: number; output: number }[] = [];
    const lost: string[] = [];
    const rows: { spendLedger?: string; spendInputTokens?: number }[] = [];
    const response = await serveNativeChat(body, new Headers(), new AbortController().signal, {
      readConfig: async () => spendConfig,
      fetch: async () => Response.json({ id: "c", object: "chat.completion", choices: [{ index: 0, message: { role: "assistant", content: "ok" }, finish_reason: "stop" }], usage: { prompt_tokens: 11, completion_tokens: 7, total_tokens: 18 } }),
      recordUsage: row => rows.push(row),
      spendReserve: async request => ({ reserved: true, sendId: request.sendId, tokens: request.inputTokens + request.outputCeilingTokens, durable: true }),
      spendAbandon: () => {},
      spendMarkDispatched: () => {},
      spendSettle: (sendId, usage) => { settled.push({ sendId, input: usage.inputTokens, output: usage.outputTokens }); },
      spendMarkLost: sendId => { lost.push(sendId); },
    });
    expect(response?.status).toBe(200);
    expect(settled).toHaveLength(1);
    expect(settled[0]).toMatchObject({ input: 11, output: 7 });
    expect(lost).toEqual([]);
    expect(rows[0]).toMatchObject({ spendLedger: "worker" });
  });

  test("a send that never reports usage becomes unresolved spend, not released", async () => {
    const lost: string[] = [];
    const response = await serveNativeChat(body, new Headers(), new AbortController().signal, {
      readConfig: async () => spendConfig,
      fetch: async () => Response.json({ id: "c", object: "chat.completion", choices: [{ index: 0, message: { role: "assistant", content: "ok" }, finish_reason: "stop" }] }),
      spendReserve: async request => ({ reserved: true, sendId: request.sendId, tokens: 0, durable: true }),
      spendAbandon: () => {},
      spendMarkDispatched: () => {},
      spendSettle: () => {},
      spendMarkLost: sendId => { lost.push(sendId); },
    });
    expect(response?.status).toBe(200);
    expect(lost).toHaveLength(1);
  });
});

describe("workerSpendTurn policy gate", () => {
  test("configures nothing without a ceiling and spends nothing without a config", async () => {
    const deps = {} as NativeChatDeps;
    expect(workerSpendTurn({}, deps, {})).toBeNull();
    expect(workerSpendTurn({ spend: {} }, deps, {})).toBeNull();
    expect(spendCeilingsConfigured(spendPolicyFromConfig(undefined))).toBe(false);
    expect(spendCeilingsConfigured(spendPolicyFromConfig({ pool: { maxTokens: 5 } }))).toBe(true);
    expect(workerSpendTurn({ spend: { pool: { maxTokens: 5 } } }, deps, {})).toBe("declined");
  });
});

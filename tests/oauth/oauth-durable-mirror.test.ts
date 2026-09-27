import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { existsSync, mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { flushConfigDirHardeningForTests } from "../../src/config/paths";
import { DOCUMENT_SEQUENCE_HEADER, documentDigest, sequenceFileFor, DURABLE_STATE_BOOT_ID_ENV, setDurableMirrorTransportForTests } from "../../src/lib/durable-mirror";
import { saveCodexAccountCredential } from "../../src/codex/account-store";
import { getAuthStorePath, getCredential, mutateStore, resetOAuthReauthReconcileStateForTests, saveCredential } from "../../src/oauth/store";
import { resetHardenedStateForTests, setAsyncIcaclsRunnerForTests, setIcaclsRunnerForTests } from "../../src/lib/windows-secret-acl";
import { removeTreeWithRetry } from "../helpers/remove-tree";

const BOOT_ID = "c".repeat(32);
const ICACLS_OK = { success: true, exitCode: 0, timedOut: false, stdout: "" };
const cred = (access: string) => ({ access, refresh: `refresh-${access}`, expires: Date.now() + 3600_000 });

let home: string;
let previousHome: string | undefined;
let previousBootId: string | undefined;

type Call = { url: string; bootId: string | null; seq: number; body: string; fileAtCall: string | null };
type Reply = number | "network" | { status: number; storedSeq: number };
let scheduled: (() => void)[] = [];

function recordingTransport(replies: Reply[]): Call[] {
  const calls: Call[] = [];
  scheduled = [];
  setDurableMirrorTransportForTests({
    origin: "http://state.test",
    sleep: async () => {},
    schedule: run => {
      scheduled.push(run);
      return { cancel: () => { scheduled = scheduled.filter(entry => entry !== run); } };
    },
    fetch: async (url, init) => {
      const path = getAuthStorePath();
      const headers = new Headers(init.headers);
      calls.push({
        url,
        bootId: headers.get("x-ocx-boot-id"),
        seq: Number(headers.get(DOCUMENT_SEQUENCE_HEADER)),
        body: String(init.body),
        fileAtCall: existsSync(path) ? readFileSync(path, "utf8") : null,
      });
      const reply = replies[calls.length - 1] ?? 204;
      if (reply === "network") throw new TypeError("fetch failed");
      if (typeof reply === "object") return new Response(null, { status: reply.status, headers: { [DOCUMENT_SEQUENCE_HEADER]: String(reply.storedSeq) } });
      return new Response(null, { status: reply });
    },
  });
  return calls;
}

const sequenceFile = () => JSON.parse(readFileSync(join(home, sequenceFileFor("auth")), "utf8"));
const flush = () => new Promise(resolve => setTimeout(resolve, 0));

describe("auth store durable mirror", () => {
  beforeEach(() => {
    previousHome = process.env.OPENCODEX_HOME;
    previousBootId = process.env[DURABLE_STATE_BOOT_ID_ENV];
    home = mkdtempSync(join(tmpdir(), "ocx-oauth-mirror-"));
    process.env.OPENCODEX_HOME = home;
    resetHardenedStateForTests();
    setIcaclsRunnerForTests(() => ICACLS_OK);
    setAsyncIcaclsRunnerForTests(async () => ICACLS_OK);
  });

  afterEach(async () => {
    setDurableMirrorTransportForTests(null);
    await flushConfigDirHardeningForTests();
    setIcaclsRunnerForTests(null);
    setAsyncIcaclsRunnerForTests(null);
    resetHardenedStateForTests();
    resetOAuthReauthReconcileStateForTests();
    if (previousHome === undefined) delete process.env.OPENCODEX_HOME;
    else process.env.OPENCODEX_HOME = previousHome;
    if (previousBootId === undefined) delete process.env[DURABLE_STATE_BOOT_ID_ENV];
    else process.env[DURABLE_STATE_BOOT_ID_ENV] = previousBootId;
    removeTreeWithRetry(home);
  });

  test("without a boot id nothing leaves the process", async () => {
    delete process.env[DURABLE_STATE_BOOT_ID_ENV];
    const calls = recordingTransport([]);
    await saveCredential("xai", cred("one"));
    expect(calls).toEqual([]);
    expect(existsSync(join(home, sequenceFileFor("auth")))).toBe(false);
    expect(getCredential("xai")?.access).toBe("one");
  });

  test("each commit reaches the durable copy before the local file, with the same bytes and a rising sequence", async () => {
    process.env[DURABLE_STATE_BOOT_ID_ENV] = BOOT_ID;
    const calls = recordingTransport([]);
    await saveCredential("xai", cred("one"));
    await saveCredential("xai", cred("two"));
    expect(calls.map(call => [call.url, call.bootId, call.seq])).toEqual([
      ["http://state.test/documents/auth", BOOT_ID, 1],
      ["http://state.test/documents/auth", BOOT_ID, 2],
    ]);
    expect(calls[0]!.fileAtCall).toBeNull();
    expect(calls[1]!.fileAtCall).toBe(calls[0]!.body);
    expect(readFileSync(getAuthStorePath(), "utf8")).toBe(calls[1]!.body);
    expect(sequenceFile()).toMatchObject({ seq: 2, mirrored: true });
  });

  test("a lost lease fails the commit at once and leaves the file untouched", async () => {
    process.env[DURABLE_STATE_BOOT_ID_ENV] = BOOT_ID;
    recordingTransport([204]);
    await saveCredential("xai", cred("one"));
    const before = readFileSync(getAuthStorePath(), "utf8");
    const calls = recordingTransport([409]);
    await expect(saveCredential("xai", cred("two"))).rejects.toThrow("state lease moved");
    expect(calls).toHaveLength(1);
    expect(readFileSync(getAuthStorePath(), "utf8")).toBe(before);
  });

  test("an unreachable Durable Object never costs the local write, and the commit is retried until it lands", async () => {
    process.env[DURABLE_STATE_BOOT_ID_ENV] = BOOT_ID;
    // Two attempts per try: the commit, then two catch-up runs.
    const calls = recordingTransport(["network", 503, 503, "network", 204]);
    // A refresh token the provider already rotated must reach the disk regardless.
    await saveCredential("xai", cred("rotated"));
    expect(getCredential("xai")?.access).toBe("rotated");
    expect(sequenceFile()).toMatchObject({ seq: 1, mirrored: false });
    expect(calls).toHaveLength(2);

    scheduled.shift()!();
    await flush();
    expect(sequenceFile()).toMatchObject({ seq: 1, mirrored: false });
    scheduled.shift()!();
    await flush();
    expect(calls.map(call => call.seq)).toEqual([1, 1, 1, 1, 1]);
    expect(calls[4]!.body).toBe(readFileSync(getAuthStorePath(), "utf8"));
    expect(sequenceFile()).toMatchObject({ seq: 1, mirrored: true });
    expect(scheduled).toEqual([]);
  });

  test("a newer commit replaces a pending retry instead of racing it", async () => {
    process.env[DURABLE_STATE_BOOT_ID_ENV] = BOOT_ID;
    const calls = recordingTransport(["network", "network"]);
    await saveCredential("xai", cred("one"));
    expect(scheduled).toHaveLength(1);
    await saveCredential("xai", cred("two"));
    expect(scheduled).toEqual([]);
    expect(calls.map(call => call.seq)).toEqual([1, 1, 2]);
    expect(sequenceFile()).toMatchObject({ seq: 2, mirrored: true });
  });

  test("a stale answer is success when it names this write, and moves past a sequence file that fell behind", async () => {
    process.env[DURABLE_STATE_BOOT_ID_ENV] = BOOT_ID;
    // The first attempt timed out after landing; its retry is told the Durable Object already has it.
    const landed = recordingTransport(["network", { status: 412, storedSeq: 1 }]);
    await saveCredential("xai", cred("one"));
    expect(landed.map(call => call.seq)).toEqual([1, 1]);
    expect(sequenceFile()).toMatchObject({ seq: 1, mirrored: true });

    setDurableMirrorTransportForTests(null);
    const behind = recordingTransport([{ status: 412, storedSeq: 9 }, 204]);
    await saveCredential("xai", cred("two"));
    expect(behind.map(call => call.seq)).toEqual([2, 10]);
    expect(sequenceFile()).toMatchObject({ seq: 10, mirrored: true });
  });

  test("a login superseded while the mirror was in flight is written nowhere", async () => {
    process.env[DURABLE_STATE_BOOT_ID_ENV] = BOOT_ID;
    const calls = recordingTransport([]);
    await saveCredential("xai", cred("kept"));
    const kept = readFileSync(getAuthStorePath(), "utf8");
    let checks = 0;
    await expect(mutateStore(store => {
      delete store.xai;
    }, [], { assertBeforePersist: () => { if (++checks > 1) throw new Error("login superseded"); } })).rejects.toThrow("login superseded");
    expect(checks).toBe(2);
    expect(readFileSync(getAuthStorePath(), "utf8")).toBe(kept);
    // The rejected store reached the Durable Object as sequence 2; the unchanged one replaces it as 3.
    expect(calls.map(call => call.seq)).toEqual([1, 2, 3]);
    expect(calls[2]!.body).toBe(kept);
    expect(sequenceFile()).toMatchObject({ seq: 3, mirrored: true });
  });
});

describe("codex account store durable mirror", () => {
  const codexFile = () => join(home, "codex-accounts.json");
  const codexSequence = () => JSON.parse(readFileSync(join(home, sequenceFileFor("codex-accounts")), "utf8"));
  const codexCred = (accessToken: string) => ({ accessToken, refreshToken: `refresh-${accessToken}`, expiresAt: Date.now() + 3600_000, chatgptAccountId: "acct-1" });

  beforeEach(() => {
    previousHome = process.env.OPENCODEX_HOME;
    previousBootId = process.env[DURABLE_STATE_BOOT_ID_ENV];
    home = mkdtempSync(join(tmpdir(), "ocx-codex-mirror-"));
    process.env.OPENCODEX_HOME = home;
    process.env[DURABLE_STATE_BOOT_ID_ENV] = BOOT_ID;
    resetHardenedStateForTests();
    setIcaclsRunnerForTests(() => ICACLS_OK);
    setAsyncIcaclsRunnerForTests(async () => ICACLS_OK);
  });

  afterEach(async () => {
    setDurableMirrorTransportForTests(null);
    await flushConfigDirHardeningForTests();
    setIcaclsRunnerForTests(null);
    setAsyncIcaclsRunnerForTests(null);
    resetHardenedStateForTests();
    if (previousHome === undefined) delete process.env.OPENCODEX_HOME;
    else process.env.OPENCODEX_HOME = previousHome;
    if (previousBootId === undefined) delete process.env[DURABLE_STATE_BOOT_ID_ENV];
    else process.env[DURABLE_STATE_BOOT_ID_ENV] = previousBootId;
    removeTreeWithRetry(home);
  });

  test("a synchronous write lands locally first, marked unmirrored until the Durable Object has it", async () => {
    const calls = recordingTransport([]);
    const sequences: unknown[] = [];
    saveCodexAccountCredential("acct", codexCred("one"));
    // Before the background mirror runs, a crash must leave the next boot preferring this file.
    sequences.push(codexSequence());
    expect(existsSync(codexFile())).toBe(true);
    await flush();
    expect(calls.map(call => [call.url, call.seq])).toEqual([["http://state.test/documents/codex-accounts", 1]]);
    expect(calls[0]!.body).toBe(readFileSync(codexFile(), "utf8"));
    expect(sequences).toMatchObject([{ seq: 1, mirrored: false }]);
    expect(codexSequence()).toEqual({ seq: 1, mirrored: true, digest: documentDigest(readFileSync(codexFile(), "utf8")) });
  });

  test("a failed background mirror is retried, and a newer write takes its place", async () => {
    const calls = recordingTransport(["network", 503]);
    saveCodexAccountCredential("acct", codexCred("one"));
    await flush();
    expect(scheduled).toHaveLength(1);
    expect(codexSequence()).toMatchObject({ seq: 1, mirrored: false });
    saveCodexAccountCredential("acct", codexCred("two"));
    expect(scheduled).toEqual([]);
    await flush();
    expect(calls.map(call => call.seq)).toEqual([1, 1, 2]);
    expect(calls[2]!.body).toBe(readFileSync(codexFile(), "utf8"));
    expect(codexSequence()).toMatchObject({ seq: 2, mirrored: true });
  });

  test("a late answer for a superseded write does not mark the newer one mirrored", async () => {
    let release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    const seqs: number[] = [];
    setDurableMirrorTransportForTests({
      origin: "http://state.test",
      sleep: async () => {},
      schedule: () => ({ cancel() {} }),
      fetch: async (_url, init) => {
        const seq = Number(new Headers(init.headers).get(DOCUMENT_SEQUENCE_HEADER));
        seqs.push(seq);
        if (seq === 1) await gate;
        return new Response(null, { status: seq === 1 ? 204 : 503 });
      },
    });
    saveCodexAccountCredential("acct", codexCred("one"));
    saveCodexAccountCredential("acct", codexCred("two"));
    await flush();
    release();
    await flush();
    await flush();
    expect(seqs).toEqual([1, 2, 2]);
    expect(codexSequence()).toMatchObject({ seq: 2, mirrored: false });
  });
});


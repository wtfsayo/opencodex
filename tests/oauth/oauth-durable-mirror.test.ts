import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { existsSync, mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { flushConfigDirHardeningForTests } from "../../src/config/paths";
import { DURABLE_STATE_BOOT_ID_ENV, setDurableMirrorTransportForTests } from "../../src/oauth/durable-mirror";
import { getAuthStorePath, getCredential, resetOAuthReauthReconcileStateForTests, saveCredential } from "../../src/oauth/store";
import { resetHardenedStateForTests, setAsyncIcaclsRunnerForTests, setIcaclsRunnerForTests } from "../../src/lib/windows-secret-acl";
import { removeTreeWithRetry } from "../helpers/remove-tree";

const BOOT_ID = "c".repeat(32);
const ICACLS_OK = { success: true, exitCode: 0, timedOut: false, stdout: "" };
const cred = (access: string) => ({ access, refresh: `refresh-${access}`, expires: Date.now() + 3600_000 });

let home: string;
let previousHome: string | undefined;
let previousBootId: string | undefined;

type Call = { url: string; bootId: string | null; body: string; fileAtCall: string | null };
function recordingTransport(statuses: (number | "network")[]): Call[] {
  const calls: Call[] = [];
  setDurableMirrorTransportForTests({
    origin: "http://state.test",
    sleep: async () => {},
    fetch: async (url, init) => {
      const path = getAuthStorePath();
      calls.push({
        url,
        bootId: new Headers(init.headers).get("x-ocx-boot-id"),
        body: String(init.body),
        fileAtCall: existsSync(path) ? readFileSync(path, "utf8") : null,
      });
      const status = statuses[calls.length - 1] ?? 204;
      if (status === "network") throw new TypeError("fetch failed");
      return new Response(null, { status });
    },
  });
  return calls;
}

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
    expect(getCredential("xai")?.access).toBe("one");
  });

  test("each commit reaches the durable copy before the local file, with the same bytes", async () => {
    process.env[DURABLE_STATE_BOOT_ID_ENV] = BOOT_ID;
    const calls = recordingTransport([]);
    await saveCredential("xai", cred("one"));
    await saveCredential("xai", cred("two"));
    expect(calls).toHaveLength(2);
    expect(calls[0]!.url).toBe("http://state.test/documents/auth");
    expect(calls[0]!.bootId).toBe(BOOT_ID);
    // The file was absent when the first write was mirrored and held the first commit during the second.
    expect(calls[0]!.fileAtCall).toBeNull();
    expect(calls[1]!.fileAtCall).toBe(calls[0]!.body);
    expect(readFileSync(getAuthStorePath(), "utf8")).toBe(calls[1]!.body);
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
    expect(getCredential("xai")?.access).toBe("one");
  });

  test("transient failures are retried; exhausting them fails the commit without writing the file", async () => {
    process.env[DURABLE_STATE_BOOT_ID_ENV] = BOOT_ID;
    const retried = recordingTransport(["network", 503, 204]);
    await saveCredential("xai", cred("one"));
    expect(retried).toHaveLength(3);
    expect(getCredential("xai")?.access).toBe("one");

    const failed = recordingTransport(["network", 502, "network"]);
    await expect(saveCredential("xai", cred("two"))).rejects.toThrow("durable auth store is unreachable");
    expect(failed).toHaveLength(3);
    expect(getCredential("xai")?.access).toBe("one");
  });
});

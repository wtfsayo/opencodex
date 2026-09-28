import { BOOT_ID_PATTERN, type DocumentCommit, type DocumentSeqs, DURABLE_DOCUMENTS, type DurableDocument, MAX_DOCUMENT_BYTES, MAX_MODEL_LIST_BYTES, type ModelList, type StoredDocument } from "./lease";

// Mirrors DOCUMENT_SEQUENCE_HEADER in src/lib/durable-mirror.ts, which the Worker bundle cannot import.
export const DOCUMENT_SEQUENCE_HEADER = "x-ocx-document-seq";

// Kept free of Workers-only imports so tests/service/cloudflare-deploy.test.ts can drive it.
export interface StateHub {
  acquireLease(bootId: string): Promise<{ granted: boolean; retryAfterSeconds?: number }>;
  renewLease(bootId: string): Promise<boolean>;
  holdsLease(bootId: string): Promise<boolean>;
  releaseLease(bootId: string): Promise<void>;
  currentSnapshot(): Promise<string | undefined>;
  commitSnapshot(bootId: string, key: string): Promise<{ replaced: string | undefined } | null>;
  readDocument(name: DurableDocument): Promise<StoredDocument | undefined>;
  commitDocument(bootId: string, name: DurableDocument, body: string, seq: number): Promise<DocumentCommit>;
  peekUsage(bootId: string, limit: number): Promise<{ seq: number; row: unknown }[] | null>;
  ackUsage(bootId: string, seqs: readonly number[]): Promise<boolean>;
  modelListCommit(bootId: string, key: string, list: ModelList, seqs: DocumentSeqs, ttlMs: number): Promise<boolean>;
}

export interface StateBucket {
  get(key: string): Promise<ReadableStream | null>;
  put(key: string, body: ReadableStream, length: number): Promise<void>;
  delete(key: string): Promise<void>;
  /** Keys under `prefix`, at most `limit` of them. */
  list(prefix: string, limit: number): Promise<string[]>;
}

const SNAPSHOT_PREFIX = "snapshots/";
const SWEEP_LIMIT = 1000;

/** Where this hub's uploads go. Keys from before namespacing (`snapshots/<bootId>/…`) sit outside it. */
export function snapshotPrefix(namespace: string): string {
  return `${SNAPSHOT_PREFIX}${namespace}/`;
}

/**
 * Deletes every object in this hub's namespace except the committed one. Safe only right after a boot
 * acquires the lease: nobody else can commit from then on, so any other object is an orphan from an
 * upload that died between put and commit, or from a failed delete of a replaced snapshot. Another
 * deployment sharing the bucket has its own namespace, and a committed key in the old layout is
 * outside every namespace, so neither is touched.
 */
export async function sweepOrphans(hub: Pick<StateHub, "currentSnapshot">, bucket: StateBucket, namespace: string): Promise<number> {
  const keep = await hub.currentSnapshot();
  const orphans = (await bucket.list(snapshotPrefix(namespace), SWEEP_LIMIT)).filter(key => key !== keep);
  for (const key of orphans) await bucket.delete(key);
  return orphans.length;
}

export async function handleStateRequest(req: Request, hub: StateHub, bucket: StateBucket, namespace: string): Promise<Response> {
  const path = new URL(req.url).pathname;
  const bootId = req.headers.get("x-ocx-boot-id") ?? "";
  if (!BOOT_ID_PATTERN.test(bootId)) return new Response("missing boot id", { status: 400 });

  if (path === "/lease" && req.method === "POST") {
    const result = await hub.acquireLease(bootId);
    if (!result.granted) {
      return new Response("lease held", { status: 409, headers: { "retry-after": String(result.retryAfterSeconds) } });
    }
    try {
      await sweepOrphans(hub, bucket, namespace);
    } catch (error) {
      // Cleanup only; never let it block a boot.
      console.error(`Snapshot orphan sweep failed: ${error instanceof Error ? error.message : String(error)}`);
    }
    return new Response(null, { status: 204 });
  }
  if (path === "/lease" && req.method === "PUT") {
    return (await hub.renewLease(bootId)) ? new Response(null, { status: 204 }) : new Response("lease lost", { status: 409 });
  }
  if (path === "/lease" && req.method === "DELETE") {
    await hub.releaseLease(bootId);
    return new Response(null, { status: 204 });
  }
  if (path === "/snapshot" && req.method === "GET") {
    // Only the lease holder restores; any other process in the container gets nothing.
    if (!(await hub.holdsLease(bootId))) return new Response("lease required", { status: 409 });
    const key = await hub.currentSnapshot();
    if (!key) return new Response("no snapshot", { status: 404 });
    const body = await bucket.get(key);
    // A committed pointer to a missing object must not read as "first boot": that would seed a
    // fresh home and upload it over the lost state.
    return body ? new Response(body) : new Response("snapshot object missing", { status: 500 });
  }
  if (path === "/snapshot" && req.method === "PUT") {
    const length = Number(req.headers.get("content-length"));
    if (!req.body || !Number.isSafeInteger(length) || length <= 0) return new Response("length required", { status: 411 });
    if (!(await hub.holdsLease(bootId))) return new Response("lease lost", { status: 409 });
    // A key per upload, never per boot: a fenced container's late upload must not overwrite the
    // object the current holder restored from, and its cleanup must delete only its own object.
    const key = `${snapshotPrefix(namespace)}${bootId}/${crypto.randomUUID()}.tar.gz`;
    await bucket.put(key, req.body, length);
    const commit = await hub.commitSnapshot(bootId, key);
    if (!commit) {
      await bucket.delete(key);
      return new Response("lease lost", { status: 409 });
    }
    if (commit.replaced) await bucket.delete(commit.replaced);
    return new Response(null, { status: 204 });
  }
  if (path === "/usage-inbox" && req.method === "GET") {
    const limit = Math.min(500, Math.max(1, Number(new URL(req.url).searchParams.get("limit")) || 500));
    const rows = await hub.peekUsage(bootId, limit);
    return rows ? Response.json({ rows }) : new Response("lease required", { status: 409 });
  }
  if (path === "/usage-inbox/ack" && req.method === "POST") {
    let seqs: unknown;
    try { seqs = ((await req.json()) as { seqs?: unknown }).seqs; } catch { seqs = undefined; }
    if (!Array.isArray(seqs) || seqs.length > 500 || !seqs.every(seq => Number.isSafeInteger(seq))) return new Response("seqs required", { status: 400 });
    return (await hub.ackUsage(bootId, seqs as number[])) ? new Response(null, { status: 204 }) : new Response("lease lost", { status: 409 });
  }
  const modelListKey = /^\/model-lists\/([0-9a-f]{64})$/.exec(path)?.[1];
  if (modelListKey !== undefined && req.method === "PUT") {
    const text = await req.text();
    if (new TextEncoder().encode(text).byteLength > MAX_MODEL_LIST_BYTES) return new Response("model list too large", { status: 413 });
    const entry = parseModelListEntry(text);
    if (!entry) return new Response("model list, headers, seqs and ttlMs required", { status: 400 });
    return (await hub.modelListCommit(bootId, modelListKey, entry.list, entry.seqs, entry.ttlMs))
      ? new Response(null, { status: 204 })
      : new Response("lease lost", { status: 409 });
  }
  const document = /^\/documents\/([a-z-]+)$/.exec(path)?.[1];
  if (document !== undefined) {
    if (!(DURABLE_DOCUMENTS as readonly string[]).includes(document)) return new Response("unknown document", { status: 404 });
    const name = document as DurableDocument;
    if (req.method === "GET") {
      if (!(await hub.holdsLease(bootId))) return new Response("lease required", { status: 409 });
      const stored = await hub.readDocument(name);
      if (!stored) return new Response("no document", { status: 404 });
      return new Response(stored.body, { headers: { "content-type": "application/json", [DOCUMENT_SEQUENCE_HEADER]: String(stored.seq) } });
    }
    if (req.method === "PUT") {
      const seq = Number(req.headers.get(DOCUMENT_SEQUENCE_HEADER));
      if (!Number.isSafeInteger(seq) || seq < 1) return new Response("sequence required", { status: 400 });
      const body = await req.text();
      if (new TextEncoder().encode(body).byteLength > MAX_DOCUMENT_BYTES) return new Response("document too large", { status: 413 });
      if (!isJsonObject(body)) return new Response("document must be a JSON object", { status: 400 });
      const commit = await hub.commitDocument(bootId, name, body, seq);
      if (commit.kind === "lease-lost") return new Response("lease lost", { status: 409 });
      if (commit.kind === "stale") return new Response("stale sequence", { status: 412, headers: { [DOCUMENT_SEQUENCE_HEADER]: String(commit.storedSeq) } });
      return new Response(null, { status: 204 });
    }
  }
  return new Response("not found", { status: 404 });
}

function parseModelListEntry(text: string): { list: ModelList; seqs: DocumentSeqs; ttlMs: number } | undefined {
  let value: { body?: unknown; headers?: unknown; seqs?: unknown; ttlMs?: unknown };
  try { value = JSON.parse(text) as typeof value; } catch { return undefined; }
  if (typeof value?.body !== "string" || !Number.isSafeInteger(value.ttlMs) || (value.ttlMs as number) <= 0) return undefined;
  if (!Array.isArray(value.headers) || !value.headers.every(pair => Array.isArray(pair) && pair.length === 2 && pair.every(part => typeof part === "string"))) return undefined;
  const seqs = value.seqs as Record<string, unknown> | undefined;
  if (!seqs || typeof seqs !== "object" || !DURABLE_DOCUMENTS.every(name => Number.isSafeInteger(seqs[name]) && (seqs[name] as number) >= 0)) return undefined;
  return {
    list: { body: value.body, headers: value.headers as [string, string][] },
    seqs: Object.fromEntries(DURABLE_DOCUMENTS.map(name => [name, seqs[name]])) as DocumentSeqs,
    ttlMs: value.ttlMs as number,
  };
}

function isJsonObject(body: string): boolean {
  try {
    const value: unknown = JSON.parse(body);
    return !!value && typeof value === "object" && !Array.isArray(value);
  } catch {
    return false;
  }
}

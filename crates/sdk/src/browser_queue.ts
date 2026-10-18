// browser_queue.js is generated from this TypeScript source. Do not edit its output.
// Concrete IndexedDB/Web Lock I/O. Rust validates raw storage metadata and owns
// every spool decision; these guards only check values needed by browser APIs.
const DATABASE = "nds-telemetry-v1";
const failure = () => new Error("telemetry_store_unavailable");
type Lease = {
  scope: string;
  release: () => void;
  deadline: number;
  timer?: number;
  held?: boolean;
};
const senders = new Map<string, Lease>();

function object(value: unknown): asserts value is Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) throw failure();
}
function text(value: unknown): string {
  if (typeof value !== "string") throw failure();
  return value;
}
function count(value: unknown): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) throw failure();
  return value;
}
function list(value: unknown): unknown[] {
  if (!Array.isArray(value)) throw failure();
  return value;
}
function bytes(value: unknown): Uint8Array {
  if (!(value instanceof Uint8Array)) throw failure();
  return value;
}
function request<T>(operation: IDBRequest<T>): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    operation.onsuccess = () => resolve(operation.result);
    operation.onerror = () => reject(failure());
  });
}
function open(timeout: number): Promise<IDBDatabase> {
  return new Promise<IDBDatabase>((resolve, reject) => {
    const operation = indexedDB.open(DATABASE, 1);
    let abandoned = false;
    const timer = setTimeout(() => {
      abandoned = true;
      reject(failure());
    }, timeout);
    operation.onupgradeneeded = () => {
      const db = operation.result;
      const records = db.createObjectStore("records", { keyPath: "id" });
      records.createIndex("scope", "scope");
      records.createIndex("expiry", "expires_at_ms");
      records.createIndex("scope_expiry", ["scope", "expires_at_ms"]);
      db.createObjectStore("health", { keyPath: "key" });
    };
    operation.onblocked = operation.onerror = () => {
      clearTimeout(timer);
      abandoned = true;
      reject(failure());
    };
    operation.onsuccess = () => {
      clearTimeout(timer);
      const db = operation.result;
      db.onversionchange = () => db.close();
      if (abandoned) db.close();
      else resolve(db);
    };
  });
}

export function releaseSender(id: string): void {
  const lease = senders.get(id);
  if (lease) {
    senders.delete(id);
    clearTimeout(lease.timer);
    lease.release();
  }
}
export function renewSender(scope: string, id: string, timeout: number): boolean {
  const lease = senders.get(id);
  const now = performance.now();
  if (!lease?.held || lease.scope !== scope || lease.deadline <= now) return false;
  lease.deadline = Math.max(lease.deadline, now + timeout);
  clearTimeout(lease.timer);
  lease.timer = setTimeout(() => releaseSender(id), lease.deadline - now);
  return true;
}
export function acquireSender(scope: string, id: string, timeout: number): Promise<boolean> {
  if (senders.has(id) || !navigator.locks) return Promise.reject(failure());
  return new Promise<boolean>((resolve, reject) => {
    const ownership: { release?: () => void } = {};
    const held = new Promise<void>((done) => {
      ownership.release = done;
    });
    const release = ownership.release;
    if (!release) throw failure();
    const lease: Lease = { scope, release, deadline: performance.now() + timeout };
    senders.set(id, lease);
    lease.timer = setTimeout(() => {
      releaseSender(id);
      reject(failure());
    }, timeout);
    navigator.locks
      .request(`nds-telemetry-send:${scope}`, { ifAvailable: true }, async (lock) => {
        if (!lock) {
          releaseSender(id);
          resolve(false);
          return;
        }
        if (senders.get(id) !== lease) throw failure();
        lease.held = true;
        resolve(true);
        await held;
        lease.held = false;
      })
      .catch(() => {
        releaseSender(id);
        reject(failure());
      });
  });
}

function metadata(record: unknown) {
  object(record);
  // Values stay unknown until the canonical Rust policy validates the snapshot.
  return {
    id: record.id,
    signal: record.signal,
    count: record.count,
    expires_at_ms: record.expires_at_ms,
    attempts: record.attempts,
    scope: record.scope,
    size: record.size,
    ...(record.inflight_until === undefined ? {} : { inflight_until: record.inflight_until }),
    byte_count: record.bytes instanceof Uint8Array ? record.bytes.length : null,
  };
}

export async function queueOperation(
  encoded: string,
  input: Uint8Array,
  policy: (snapshot: string) => string,
): Promise<string | [string, Uint8Array]> {
  // Copy before the first await: borrowed WASM memory can grow meanwhile.
  const payload = new Uint8Array(input);
  const read: unknown = JSON.parse(encoded);
  object(read);
  const scope = text(read.scope);
  const lookupId = read.lookup == null ? null : text(read.lookup);
  const senderId = read.sender == null ? null : text(read.sender);
  const timeout = count(read.timeout_ms);
  let db: IDBDatabase | undefined;
  let tx: IDBTransaction | undefined;
  let timer: number | undefined;
  let done: Promise<void> | undefined;
  try {
    db = await open(timeout);
    const transaction = db.transaction(["records", "health"], "readwrite", {
      durability: "strict",
    });
    tx = transaction;
    done = new Promise<void>((resolve, reject) => {
      transaction.oncomplete = () => resolve();
      transaction.onabort = transaction.onerror = () => reject(failure());
    });
    done.catch(() => {});
    timer = setTimeout(() => {
      try {
        transaction.abort();
      } catch {}
    }, timeout);
    const records = transaction.objectStore("records");
    const health = transaction.objectStore("health");
    const now = Date.now();
    const [values, total, expired, rows, lookup] = await Promise.all([
      request<unknown[]>(health.getAll(undefined, count(read.health_limit))),
      request(records.count()),
      request<unknown[]>(
        records.index("expiry").getAll(IDBKeyRange.upperBound(now), count(read.row_limit)),
      ),
      read.scan === true
        ? request<unknown[]>(
            read.ordered === true
              ? records
                  .index("scope_expiry")
                  .getAll(
                    IDBKeyRange.bound(
                      [scope, now],
                      [scope, count(read.counter_limit)],
                      true,
                      false,
                    ),
                    count(read.next_limit),
                  )
              : records.index("scope").getAll(scope, count(read.row_limit)),
          )
        : [],
      lookupId ? request<unknown>(records.get(lookupId)) : null,
    ]);
    const loaded = new Map<string, Record<string, unknown>>(
      [...expired, ...rows, ...(lookup ? [lookup] : [])].map(
        (row): [string, Record<string, unknown>] => {
          object(row);
          return [text(row.id), row];
        },
      ),
    );
    // No await separates the synchronous Rust decision from its mutations.
    const sender = senderId ? senders.get(senderId) : undefined;
    const plan: unknown = JSON.parse(
      policy(
        JSON.stringify({
          now,
          health: values,
          record_count: total,
          expired: expired.map(metadata),
          records: rows.map(metadata),
          lookup: lookup ? metadata(lookup) : null,
          held_scope: sender?.held ? sender.scope : null,
        }),
      ),
    );
    object(plan);
    const combine = list(plan.combine);
    if (combine.length) {
      const parts = combine.map((id) => bytes(loaded.get(text(id))?.bytes));
      const joined = new Uint8Array(parts.reduce((size, part) => size + part.length, 0));
      let offset = 0;
      for (const part of parts) {
        joined.set(part, offset);
        offset += part.length;
      }
      object(plan.next);
      const id = text(plan.next.id);
      const previous = loaded.get(id);
      if (!previous) throw failure();
      loaded.set(id, { ...previous, bytes: joined });
    }
    for (const id of list(plan.remove)) records.delete(text(id));
    for (const row of list(plan.put)) {
      object(row);
      const previous = loaded.get(text(row.id));
      if (!previous) throw failure();
      records.put({ ...previous, ...row });
    }
    if (plan.append !== null) {
      object(plan.append);
      records.add({ ...plan.append, bytes: payload });
    }
    for (const row of list(plan.health)) health.put(row);
    if (plan.global !== null) {
      object(plan.global);
      health.put(plan.global);
    }
    await done;
    if (plan.next !== null) {
      object(plan.next);
      return [JSON.stringify(plan.next), bytes(loaded.get(text(plan.next.id))?.bytes)];
    }
    return JSON.stringify(plan.result);
  } catch {
    if (tx)
      try {
        tx.abort();
      } catch {}
    if (done) await done.catch(() => {});
    throw failure();
  } finally {
    clearTimeout(timer);
    db?.close();
  }
}

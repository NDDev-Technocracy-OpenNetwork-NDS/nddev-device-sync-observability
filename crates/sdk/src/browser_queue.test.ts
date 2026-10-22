// Unit tests for the sender-lease lifecycle. IndexedDB access stays covered by
// the owned backend acceptance; these tests exercise the Web Locks admission
// and lease rules with a stubbed lock manager.
import { beforeEach, describe, expect, test } from "bun:test";
import { acquireSender, releaseSender, renewSender } from "./browser_queue";

const SCOPE = "telemetry";
const ID = "sender-1";

type Grant = { name: string } | null;

function installLocks(grant: Grant = { name: "lock" }) {
  Object.defineProperty(globalThis, "navigator", {
    value: {
      locks: {
        request: (
          _name: string,
          _options: { ifAvailable: boolean },
          run: (lock: Grant) => unknown,
        ) => run(grant),
      },
    },
    configurable: true,
  });
}

function removeLocks() {
  Object.defineProperty(globalThis, "navigator", {
    value: {},
    configurable: true,
  });
}

beforeEach(() => {
  releaseSender(ID);
  installLocks();
});

describe("acquireSender", () => {
  test("grants a held lease while the platform lock is held", async () => {
    await expect(acquireSender(SCOPE, ID, 60_000)).resolves.toBe(true);
    expect(renewSender(SCOPE, ID, 60_000)).toBe(true);
  });

  test("reports no grant when the platform denies the lock", async () => {
    installLocks(null);
    await expect(acquireSender(SCOPE, ID, 60_000)).resolves.toBe(false);
  });

  test("rejects when a lease for the id is already registered", async () => {
    await acquireSender(SCOPE, ID, 60_000);
    await expect(acquireSender(SCOPE, ID, 60_000)).rejects.toThrow("telemetry_store_unavailable");
  });

  test("rejects when the runtime exposes no Web Locks", async () => {
    removeLocks();
    await expect(acquireSender(SCOPE, ID, 60_000)).rejects.toThrow("telemetry_store_unavailable");
  });
});

describe("renewSender", () => {
  test("renews only the matching scope of a held lease", async () => {
    await acquireSender(SCOPE, ID, 60_000);
    expect(renewSender("other-scope", ID, 60_000)).toBe(false);
    expect(renewSender(SCOPE, ID, 60_000)).toBe(true);
  });

  test("does not renew an unknown id", () => {
    expect(renewSender(SCOPE, "unknown", 60_000)).toBe(false);
  });
});

describe("releaseSender", () => {
  test("ends renewal and frees the id for reacquisition", async () => {
    await acquireSender(SCOPE, ID, 60_000);
    releaseSender(ID);
    expect(renewSender(SCOPE, ID, 60_000)).toBe(false);
    await expect(acquireSender(SCOPE, ID, 60_000)).resolves.toBe(true);
  });
});

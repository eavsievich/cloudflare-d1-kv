import {describe, expect, test, vi} from "vitest";
import {createTimeFunction, provideTestKv} from "./common";
import Database from "better-sqlite3";
import {KvBetterSqliteBackend} from "../kv";

async function getFirst(kvBackend: KvBetterSqliteBackend) {
  return await kvBackend.first("select * from kv");
}

describe("cleanup tests", () => {
  test("clean never triggered with threshold=0", async () => {
    // Initial time is 0
    const timeFunction = createTimeFunction(0);

    // Set up db
    const db = new Database(":memory:");
    const kvBackend = new KvBetterSqliteBackend(db);
    const kv = provideTestKv(timeFunction, 0, db);

    // Set test key with expiration in 1 second
    await kv.set(["abc"], "def", {ex: 1});

    // Should not clean expired key
    timeFunction.mockImplementation(() => 10);
    await kv.get(["abc"]);

    expect(await getFirst(kvBackend)).toBeDefined();
  });

  test("clean always triggered with threshold=1", async () => {
    // Initial time is 0
    const timeFunction = createTimeFunction(0);

    // Set up db, threshold=1
    const db = new Database(":memory:");
    const kvBackend = new KvBetterSqliteBackend(db);
    const kv = provideTestKv(timeFunction, 1, db);

    // Set test key with expiration in 1 second
    await kv.set(["abc"], "def", {ex: 1});

    // Should clean expired key
    timeFunction.mockImplementation(() => 10);
    await kv.get(["abc"]);

    expect(await getFirst(kvBackend)).toBeUndefined();
  });
});

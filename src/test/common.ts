import Database from "better-sqlite3";
import {runSqliteMigrations} from "../sqlite_migration";
import {Kv, KvBetterSqliteBackend, SqliteDatabase} from "../kv";
import {vi} from "vitest";

export function provideTestKv(
  currentTimeProvider: () => number = () => 0,
  clearExpiredThreshold: number = 0.1,
  db: SqliteDatabase = new Database(":memory:"),
) {
  runSqliteMigrations(db, "kv");
  return new Kv("kv", new KvBetterSqliteBackend(db), clearExpiredThreshold, currentTimeProvider);
}

export function createTimeFunction(initialTime: number) {
  return vi.fn(() => initialTime);
}
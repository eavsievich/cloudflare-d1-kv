import {D1Database} from "@cloudflare/workers-types";

export type KvKeyPart = string | number | true | false;

export type KvKey = Array<KvKeyPart>;

export type KvSortOrder = "asc" | "desc";

export type KvSortTrait = "key" | "created_at" | "updated_at";

export interface KvSetOptions {
  ex?: number; // Set the specified expire time, in seconds.
  nx?: boolean; // Only set the key if it does not already exist.
  get?: boolean; // Return the old string stored at key, or nil if key did not exist.
}

export interface KvDelOptions {
  get: boolean; // Return the old string stored at key, or nil if key did not exist.
}

export interface KvListOptions {
  prefix: KvKey;
  offset: number;
  limit: number;
  order?: KvSortOrder; // asc by default
  sortTrait?: KvSortTrait; // key by default
}

export interface KvResult<T> {
  key: KvKey;
  value?: T;
  createdAt?: number;
  updatedAt?: number;
  expiresAt?: number;
}

export interface KvBackend {
  first(sql: string, ...parameters: unknown[]): Promise<KvRawResult | null>;

  all(sql: string, ...parameters: unknown[]): Promise<KvRawResult[]>;

  run(sql: string, ...parameters: unknown[]): Promise<void>;
}

export class KvD1Backend implements KvBackend {

  constructor(private readonly db: D1Database) {
  }

  async first(sql: string, ...parameters: unknown[]): Promise<KvRawResult | null> {
    return await this.db
      .prepare(sql)
      .bind(...parameters)
      .first<KvRawResult>();
  }

  async all(sql: string, ...parameters: unknown[]): Promise<KvRawResult[]> {
    const r = await this.db
      .prepare(sql)
      .bind(...parameters)
      .all<KvRawResult>();
    if (!r.success) {
      throw new Error(`failed to execute sql ${r.error}`);
    }
    return r.results || [];
  }

  async run(sql: string, ...parameters: unknown[]): Promise<void> {
    await this.db
      .prepare(sql)
      .bind(...parameters)
      .run();
  }
}

export class KvBetterSqliteBackend implements KvBackend {

  constructor(private readonly db: SqliteDatabase) {
  }

  first(sql: string, ...parameters: unknown[]): Promise<KvRawResult | null> {
    return Promise.resolve(
      this.db
        .prepare(sql)
        .get(parameters) as KvRawResult | null
    );
  }

  async all(sql: string, ...parameters: unknown[]): Promise<KvRawResult[]> {
    const result = this.db
      .prepare(sql)
      .all(parameters);
    return Promise.resolve((result || []) as KvRawResult[]);
  }

  async run(sql: string, ...parameters: unknown[]): Promise<void> {
    this.db
      .prepare(sql)
      .run(parameters);
  }
}

export interface KvRawResult {
  key: string;
  value: string;
  created_at: number;
  updated_at: number;
  expires_at: number;
}

export class Kv {

  constructor(
    private tableName: string,
    private readonly db: KvBackend,
    private readonly clearExpiredThreshold: number = 0.1,
    private readonly currentTimeProvider: () => number = currentTimeSeconds,
  ) {
  }

  async get<T>(key: KvKey): Promise<KvResult<T>> {
    await this.clearExpired();
    const wrappedKey = wrapKey(key);
    const result = await this.db.first(sqlGet(this.tableName), wrappedKey, this.currentTimeProvider());
    return transformResult<T>(wrappedKey, result);
  }

  async set<T>(key: KvKey, value: T, options?: KvSetOptions): Promise<KvResult<T>> {
    if (options && (options.get || options.nx)) {
      if (options.ex === 0) {
        throw new Error("ex must be greater than 0");
      }
      const storedValue = await this.get<T>(key);
      if (!options.nx || (options.nx && !storedValue.value)) {
        await this.setInternal(key, value, options);
      }
      if (options.get) {
        return storedValue;
      } else {
        return {key: key};
      }
    } else {
      await this.setInternal(key, value, options);
      return {key: key};
    }
  }

  async del<T>(key: KvKey, options?: KvDelOptions): Promise<KvResult<T>> {
    if (options && options.get) {
      const storedValue = await this.get<T>(key);
      await this.delInternal(key);
      return storedValue;
    } else {
      await this.delInternal(key);
      return {key: key};
    }
  }

  async list<T>(listOptions: KvListOptions): Promise<Array<KvResult<T>>> {
    const r = await this.db.all(
      sqlList(this.tableName, listOptions.sortTrait || "key", listOptions.order || "asc"),
      wrapKeyPrefix(listOptions.prefix),
      this.currentTimeProvider(),
      listOptions.limit,
      listOptions.offset,
    );
    return r.map(value => {
      return transformResult(value.key, value);
    });
  }

  private async delInternal(key: KvKey) {
    await this.db.run(sqlDel(this.tableName), wrapKey(key));
  }

  private async setInternal<T>(key: KvKey, value: T, options?: KvSetOptions) {
    const wrappedKey = wrapKey(key);
    const wrappedValue = JSON.stringify(value);
    const ts = this.currentTimeProvider();
    const expiresAt = options?.ex ? ts + options?.ex : -1;
    await this.db.run(
      sqlSet(this.tableName),
      wrappedKey,
      wrappedValue,
      ts,
      ts,
      expiresAt,
      wrappedValue,
      ts,
      expiresAt,
    );
  }

  private async clearExpired() {
    if (randomCheck(this.clearExpiredThreshold)) {
      await this.db.run(sqlClearExpired(this.tableName), this.currentTimeProvider());
    }
  }
}

function wrapKey(key: KvKey) {
  if (key.length === 0) {
    throw new Error("key must not be empty");
  }
  return JSON.stringify(key);
}

function wrapKeyPrefix(key: KvKey) {
  const wrappedKey = wrapKey(key);
  return `${wrappedKey.substring(0, wrappedKey.length - 1)},%`;
}

function unwrapKey(key: string) {
  return JSON.parse(key) as KvKey;
}

function currentTimeSeconds() {
  return Math.floor(new Date().getTime() / 1000);
}

function transformResult<T>(packedKey: string, kvRawResult: KvRawResult | null): KvResult<T> {
  if (!kvRawResult) {
    return <KvResult<T>>{
      key: unwrapKey(packedKey),
    };
  } else {
    return {
      key: unwrapKey(packedKey),
      value: JSON.parse(kvRawResult.value),
      createdAt: kvRawResult.created_at,
      updatedAt: kvRawResult.updated_at,
      expiresAt: kvRawResult.expires_at,
    };
  }
}

function randomCheck(threshold: number) {
  if (threshold < 0 || threshold > 1) {
    throw new Error(`threshold must be between 0 and 1, but was ${threshold}`);
  }
  return Math.random() < threshold;
}

function sqlGet(tableName: string) {
  return `select * from "${tableName}" where "key" = ? and ("expires_at" = -1 or "expires_at" > ?)`;
}

function sqlSet(tableName: string) {
  return `insert into "${tableName}" ("key", "value", "created_at", "updated_at", "expires_at") values (?, ?, ?, ?, ?) on conflict ("key") do update set "value" = ?, "updated_at" = ?, "expires_at" = ?`;
}

function sqlDel(tableName: string) {
  return `delete from "${tableName}" where "key" = ?`;
}

function sqlList(tableName: string, trait: KvSortTrait, order: KvSortOrder) {
  return `select * from "${tableName}" where "key" like ? and ("expires_at" = -1 or "expires_at" > ?) order by "${trait}" ${order} limit ? offset ?`;
}

function sqlClearExpired(tableName: string) {
  return `delete from "${tableName}" where "expires_at" != -1 and "expires_at" < ?`;
}

/**
 * This interface is the subset of better-sqlite3 driver's `Database` class that
 * we need.
 *
 * We don't use the type from `better-sqlite3` here to not have a dependency to it.
 *
 * https://github.com/JoshuaWise/better-sqlite3/blob/master/docs/api.md#new-databasepath-options
 */
export interface SqliteDatabase {
  close(): void;

  prepare(sql: string): SqliteStatement;
}

export interface SqliteStatement {
  readonly reader: boolean;

  get(parameters: ReadonlyArray<unknown>): unknown;

  all(parameters: ReadonlyArray<unknown>): unknown[];

  run(parameters: ReadonlyArray<unknown>): {
    changes: number | bigint
    lastInsertRowid: number | bigint
  };
}
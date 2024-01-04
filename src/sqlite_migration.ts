import {SqliteDatabase} from "./kv";

export function runSqliteMigrations(db: SqliteDatabase, tableName: string) {
  const migrations = [
    `DROP TABLE IF EXISTS "${tableName}";`,
    `CREATE TABLE IF NOT EXISTS "${tableName}" (
        "key" text not null primary key,
        "value" text not null,
        "created_at" integer not null,
        "updated_at" integer not null,
        "expires_at" integer not null
    );`,
    `CREATE INDEX "created_at_idx" on "${tableName}" ("created_at");`,
    `CREATE INDEX "updated_at_idx" on "${tableName}" ("updated_at");`,
    `CREATE INDEX "expires_at_idx" on "${tableName}" ("expires_at");`,
  ];
  migrations.forEach(value => {
    db.prepare(value).run([]);
  });
}


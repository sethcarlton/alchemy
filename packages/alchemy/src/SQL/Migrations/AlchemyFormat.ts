import * as Effect from "effect/Effect";
import {
  convertedRowInsertSql,
  findForeignHistory,
  matchForeignRows,
  toTimestampString,
} from "./Convert.ts";
import {
  MigrationError,
  MigrationHistoryConflictError,
  type MigrationDialect,
  type MigrationRecord,
  type SqlExecutor,
} from "./Format.ts";
import { classifyTable, tableColumns } from "./Introspect.ts";
import { qualifyTable, quoteIdentifier, sqlLiteral } from "./Records.ts";

export const ALCHEMY_DEFAULT_SCHEMA = "alchemy";
export const ALCHEMY_DEFAULT_TABLE = "__alchemy_migrations";

/**
 * THE Alchemy applied-migrations table: `id, hash, created_at, name,
 * applied_at`, name-keyed detection. This is deliberately drizzle's column
 * shape — a database whose history lives in `__drizzle_migrations` adopts
 * with a verbatim row copy — but Alchemy owns the table and it is the only
 * format Alchemy ever writes. Migrating from drizzle/prisma/wrangler
 * bookkeeping is a one-way conversion performed once (see `Convert.ts`).
 */
const createTableSql = (
  table: string,
  dialect: MigrationDialect,
  schema?: string,
): string => {
  const quoted = qualifyTable(table, dialect, schema);
  switch (dialect) {
    case "sqlite":
      return `CREATE TABLE IF NOT EXISTS ${quoted} (
  id INTEGER PRIMARY KEY,
  hash text NOT NULL,
  created_at numeric,
  name text,
  applied_at TEXT
);`;
    case "postgres":
      return `CREATE TABLE IF NOT EXISTS ${quoted} (
  id SERIAL PRIMARY KEY,
  hash text NOT NULL,
  created_at bigint,
  name text,
  applied_at timestamp with time zone DEFAULT now()
);`;
    case "mysql":
      return `CREATE TABLE IF NOT EXISTS ${quoted} (
  id SERIAL PRIMARY KEY,
  hash TEXT NOT NULL,
  created_at BIGINT,
  name TEXT,
  applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);`;
  }
};

const insertSql = (
  table: string,
  dialect: MigrationDialect,
  record: Pick<MigrationRecord, "name" | "hash" | "createdAtMillis">,
  schema?: string,
): string => {
  const quoted = qualifyTable(table, dialect, schema);
  const applied = dialect === "sqlite" ? ", datetime('now')" : "";
  const appliedColumn = dialect === "sqlite" ? ", applied_at" : "";
  return `INSERT INTO ${quoted} (hash, created_at, name${appliedColumn}) VALUES (${sqlLiteral(record.hash)}, ${sqlLiteral(record.createdAtMillis ?? null)}, ${sqlLiteral(record.name)}${applied});`;
};

const renameSql = (
  from: string,
  to: string,
  dialect: MigrationDialect,
  schema?: string,
): string =>
  dialect === "mysql"
    ? `RENAME TABLE ${qualifyTable(from, dialect, schema)} TO ${quoteIdentifier(to, dialect)};`
    : `ALTER TABLE ${qualifyTable(from, dialect, schema)} RENAME TO ${quoteIdentifier(to, dialect)};`;

/**
 * Rebuild an in-place table (legacy Alchemy 3-column / oldest 2-column /
 * wrangler-shaped) into the Alchemy shape, backfilling `hash` and
 * `created_at` from local records matched by name. A recorded row with no
 * matching local file is a hard error — mirrors the same rule conversion
 * applies (see `matchForeignRows`).
 */
const rebuildInPlace = (options: {
  executor: SqlExecutor;
  table: string;
  records: ReadonlyArray<MigrationRecord>;
  /** SQL expression yielding the migration name from the old table. */
  nameExpr: string;
  tool: "legacy-alchemy" | "wrangler";
  schema?: string;
}) =>
  Effect.gen(function* () {
    const { executor, table, records, nameExpr, schema } = options;
    const dialect = executor.dialect;
    const quoted = qualifyTable(table, dialect, schema);
    const rows = yield* executor.query(
      `SELECT ${nameExpr} AS name, applied_at FROM ${quoted} ORDER BY id;`,
    );
    const matched = yield* matchForeignRows({
      history: {
        tool: options.tool,
        source: table,
        rows: rows
          .filter((row) => row.name !== null && row.name !== undefined)
          .map((row) => ({
            name: String(row.name),
            hash: undefined,
            createdAtMillis: undefined,
            appliedAt: toTimestampString(row.applied_at),
          })),
      },
      records,
    });

    const temp = `${table}_alchemy_upgrade`;
    yield* executor.batch([
      `DROP TABLE IF EXISTS ${qualifyTable(temp, dialect, schema)};`,
      createTableSql(temp, dialect, schema).replace(
        "CREATE TABLE IF NOT EXISTS",
        "CREATE TABLE",
      ),
      ...matched.map((row) =>
        convertedRowInsertSql(temp, dialect, row, schema),
      ),
      `DROP TABLE ${quoted};`,
      renameSql(temp, table, dialect, schema),
    ]);
  });

const ensureTable = (options: {
  executor: SqlExecutor;
  table: string;
  records: ReadonlyArray<MigrationRecord>;
  schema?: string;
}) =>
  Effect.gen(function* () {
    const { executor, table, records, schema } = options;
    if (schema && executor.dialect === "postgres") {
      yield* executor.batch([
        `CREATE SCHEMA IF NOT EXISTS ${quoteIdentifier(schema, executor.dialect)};`,
      ]);
    }
    const shape = classifyTable(yield* tableColumns(executor, table, schema));
    switch (shape) {
      case "absent": {
        // Greenfield for us — but possibly not for the database. Adopt any
        // history the previous tool (drizzle-kit / prisma / wrangler) left
        // behind: copy it into our table ONCE and freeze theirs. One-way.
        const history = yield* findForeignHistory({ executor, table, schema });
        const converted = history
          ? yield* matchForeignRows({ history, records })
          : [];
        yield* executor.batch([
          createTableSql(table, executor.dialect, schema),
          ...converted.map((row) =>
            convertedRowInsertSql(table, executor.dialect, row, schema),
          ),
        ]);
        return;
      }
      case "drizzle-shaped":
        // Already our column shape (shared with drizzle v1) — including
        // the case where the user pointed `table` straight at an existing
        // `__drizzle_migrations`. Adopt in place.
        return;
      case "legacy-alchemy":
        yield* rebuildInPlace({
          executor,
          table,
          records,
          nameExpr: "name",
          tool: "legacy-alchemy",
          schema,
        });
        return;
      case "legacy-2col":
        yield* rebuildInPlace({
          executor,
          table,
          records,
          nameExpr: "id",
          tool: "legacy-alchemy",
          schema,
        });
        return;
      case "wrangler":
        // A wrangler table at OUR resolved table name (e.g. legacy D1
        // state pinned to `d1_migrations`, or a wrangler user's table
        // adopted under an explicit `table:`): convert it in place.
        yield* rebuildInPlace({
          executor,
          table,
          records,
          nameExpr: "name",
          tool: "wrangler",
          schema,
        });
        return;
      case "unknown":
        return yield* new MigrationError({
          message:
            `Migrations table "${table}" has an unrecognized column layout; ` +
            `refusing to write bookkeeping into it.`,
        });
    }
  });

const appliedNames = (executor: SqlExecutor, table: string, schema?: string) =>
  executor
    .query(`SELECT name FROM ${qualifyTable(table, executor.dialect, schema)};`)
    .pipe(
      Effect.map(
        (rows) =>
          new Set(
            rows
              .map((row) => row.name)
              .filter((name) => name !== null && name !== undefined)
              .map(String),
          ),
      ),
    );

/**
 * Apply pending migrations with Alchemy's bookkeeping. Idempotent: each
 * migration's statements and its bookkeeping INSERT go through
 * `executor.batch` as one unit (a transaction on pg/mysql, one batched
 * query on D1, which has no transactions over HTTP).
 *
 * Applied-detection is name-keyed with layout aliasing: pre-registry
 * Alchemy recorded drizzle-layout migrations under `<dir>/migration.sql`
 * while current records key them by `<dir>`, so both keys are honored.
 */
export const applyAlchemyFormat = (options: {
  executor: SqlExecutor;
  table: string;
  records: ReadonlyArray<MigrationRecord>;
  schema?: string;
}): Effect.Effect<void, MigrationError | MigrationHistoryConflictError> =>
  Effect.gen(function* () {
    const { executor, table, records, schema } = options;
    if (records.length === 0) return;
    yield* ensureTable({ executor, table, records, schema });
    const applied = yield* appliedNames(executor, table, schema);
    for (const record of records) {
      if (
        applied.has(record.name) ||
        applied.has(`${record.name}/migration.sql`) ||
        applied.has(record.name.replace(/\/migration\.sql$/, ""))
      ) {
        continue;
      }
      yield* executor.batch([
        ...record.statements,
        insertSql(table, executor.dialect, record, schema),
      ]);
    }
  });

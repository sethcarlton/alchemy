import {
  findForeignHistory,
  type SqlExecutor,
} from "@/SQL/Migrations/index.ts";
import { expect, test } from "alchemy-test";
import * as Effect from "effect/Effect";

test.effect(
  "discovers Prisma history when the target uses the same table in another schema",
  () =>
    Effect.gen(function* () {
      const executor: SqlExecutor = {
        dialect: "postgres",
        query: (sql) => {
          if (sql.includes("table_name = '_prisma_migrations'")) {
            return Effect.succeed([
              { name: "migration_name", type: "text" },
              { name: "checksum", type: "text" },
            ]);
          }
          if (sql.includes('FROM "_prisma_migrations"')) {
            return Effect.succeed([
              {
                migration_name: "20260101000000_init",
                checksum: "hash",
                finished_at: "2026-01-01T00:00:00.000Z",
                rolled_back_at: null,
              },
            ]);
          }
          return Effect.succeed([]);
        },
        batch: () => Effect.void,
      };

      const history = yield* findForeignHistory({
        executor,
        table: "_prisma_migrations",
        schema: "internal",
      });

      expect(history?.tool).toBe("prisma");
      expect(history?.rows.map((row) => row.name)).toEqual([
        "20260101000000_init",
      ]);
    }),
);

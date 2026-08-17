import type {
  MigrationsInput,
  PostgresMigrationsInput,
} from "@/SQL/Migrations/index.ts";

const postgres: PostgresMigrationsInput = {
  dir: "./migrations",
  schema: "alchemy",
};

// @ts-expect-error SQL dialects without schemas must reject this option.
const sqlite: MigrationsInput = postgres;

void sqlite;

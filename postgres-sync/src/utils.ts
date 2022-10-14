import {
  Scalar,
  getEnv,
  strictParseInt,
  tryGetEnv,
} from "@dataland-io/dataland-sdk";
import { Client } from "@dataland-workerlibs/postgres";
import { oneLine } from "common-tags";
import { Column, PrimaryKeyColumn, TableMapping } from "./types";

export const getDoWriteback = (): boolean => {
  return tryGetEnv("PG_DO_WRITEBACK") === "true";
};

export const getTableMapping = (): TableMapping => {
  const mysqlTableMapping = getEnv("PG_TABLE_MAPPING");
  const tableMappingJson = JSON.parse(mysqlTableMapping);
  const tableMapping = TableMapping.parse(tableMappingJson);

  const seenTargetTableNames = new Set<string>();

  for (const sourceTableName in tableMapping) {
    validatePgIdentifier(sourceTableName);

    const targetTableName = tableMapping[sourceTableName];
    const targetTableNameIsValid = /^[a-z][_a-z0-9]{0,62}$/.test(
      targetTableName
    );
    if (!targetTableNameIsValid) {
      throw new Error(
        `Invalid Dataland table name - ${targetTableNameIsValid}`
      );
    }

    // Don't allow multiple source tables to map into the same target table.
    // In theory we can handle this but we do not currently for the sake of simplicity.
    if (seenTargetTableNames.has(targetTableName)) {
      throw new Error(
        `Invalid table mapping - duplicate target table - ${targetTableName}`
      );
    }
    seenTargetTableNames.add(targetTableName);
  }

  return tableMapping;
};

export const getPgSchema = (): string => {
  const pgSchema = tryGetEnv("PG_SCHEMA");
  if (pgSchema == null) {
    return "public";
  }
  validatePgIdentifier(pgSchema);
  return pgSchema;
};

const validatePgIdentifier = (identifier: string) => {
  // We allow every character except double quotes, because we will always quote the identifiers
  // when constructing queries.
  //
  // From https://www.postgresql.org/docs/current/sql-syntax-lexical.html:
  // Quoted identifiers can contain any character, except the character with code zero.
  // This allows constructing table or column names that would otherwise not be possible,
  // such as ones containing spaces or ampersands. The length limitation still applies.
  if (identifier.includes(`"`)) {
    throw new Error(
      `Invalid Postgres identifier - contains double-quote character - ${identifier}`
    );
  }
  if (identifier.length > 63) {
    throw new Error(`Invalid Postgres identifier - too long - ${identifier}`);
  }
};

export const getClient = () => {
  const pgHost = getEnv("PG_HOST");
  const pgPort = (() => {
    const str = tryGetEnv("PG_PORT");
    if (str == null) {
      return 6543;
    }
    const int = strictParseInt(str);
    if (int == null) {
      throw new Error(`invalid PG_PORT - ${str}`);
    }
    return int;
  })();
  const pgDatabase = tryGetEnv("PG_DATABASE") ?? "postgres";
  const pgUser = tryGetEnv("PG_USER") ?? "postgres";
  const pgPassword = getEnv("PG_PASSWORD");

  const client = new Client({
    hostname: pgHost,
    port: pgPort,
    database: pgDatabase,
    user: pgUser,
    password: pgPassword,
    tls: {
      enabled: false,
      // enforce: false,
      // caCertificates: [],
    },
    connection: {
      attempts: 1,
    },
  });
  return client;
};

export const getPrimaryKeyColumns = async (
  client: Client,
  pgSchema: string,
  tableNames: string[]
): Promise<Map<string, Map<string, PrimaryKeyColumn>>> => {
  const tables: Map<string, Map<string, PrimaryKeyColumn>> = new Map();
  for (const tableName of tableNames) {
    const result = await client.queryArray<[string, number]>(
      oneLine`
        select a.column_name, a.ordinal_position
        from
        (
          select constraint_name, column_name, ordinal_position from information_schema.key_column_usage
          where table_schema = $1 and table_name = $2
        ) a
        inner join
        (
          select constraint_name from information_schema.table_constraints
          where table_schema = $1 and table_name = $2 and constraint_type = 'PRIMARY KEY'
        ) b
        on a.constraint_name = b.constraint_name
        order by a.ordinal_position
      `,
      [pgSchema, tableName]
    );
    const columns = new Map<string, PrimaryKeyColumn>();
    for (const row of result.rows) {
      const [column_name, ordinal_position] = row;
      const column: PrimaryKeyColumn = {
        table_name: tableName,
        column_name,
        ordinal_position,
      };
      columns.set(column_name, column);
    }
    tables.set(tableName, columns);
  }
  return tables;
};

export const getColumns = async (
  client: Client,
  pgSchema: string,
  tableNames: string[]
): Promise<Map<string, Map<string, Column>>> => {
  const result = await client.queryArray<
    [string, string, number, string, string]
  >(
    oneLine`
      select table_name, column_name, ordinal_position, data_type, is_nullable
      from information_schema.columns
      where table_schema = $1 and table_name = any($2::text[])
      order by table_name, ordinal_position
    `,
    [pgSchema, `{${tableNames.map((t) => `"${t}"`).join(",")}}`]
  );
  const tables: Map<string, Map<string, Column>> = new Map();
  for (const row of result.rows) {
    const [table_name, column_name, ordinal_position, data_type, is_nullable] =
      row;
    let columns: Map<string, Column> | undefined = tables.get(table_name);
    if (columns == null) {
      columns = new Map();
      tables.set(table_name, columns);
    }
    const column: Column = {
      table_name,
      column_name,
      ordinal_position,
      data_type,
      is_nullable,
    };
    columns.set(column_name, column);
  }
  return tables;
};

export const postprocessRows = (rows: Record<string, unknown>[]) => {
  for (const row of rows) {
    for (const key in row) {
      const value = row[key];
      if (isScalar(value)) {
        // continue
      } else if (typeof value === "bigint") {
        row[key] = Number(value);
      } else if (value instanceof Date) {
        row[key] = value.toISOString();
      } else {
        console.warn(
          "Found non-scalar value, converting to string",
          key,
          typeof value,
          row
        );
        row[key] = String(value);
      }
    }
  }
};

// TODO(hzuo): Remove once this is exported from dataland-sdk (probably 0.18.0)
export const isScalar = (unknownValue: unknown): unknownValue is Scalar => {
  return (
    unknownValue === null || // note the triple equals here instead of the usual `== null`
    typeof unknownValue === "string" ||
    typeof unknownValue === "number" ||
    typeof unknownValue === "boolean"
  );
};

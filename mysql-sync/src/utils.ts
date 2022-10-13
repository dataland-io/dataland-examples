import {
  Scalar,
  getEnv,
  strictParseInt,
  tryGetEnv,
} from "@dataland-io/dataland-sdk";
import { Client } from "@dataland-workerlibs/mysql";
import { oneLine } from "common-tags";
import { z } from "zod";
import { Column, PrimaryKeyColumn, TableMapping } from "./types";

export const getDoWriteback = (): boolean => {
  return tryGetEnv("DO_WRITEBACK") === "true";
};

export const getTableMapping = (): TableMapping => {
  const mysqlTableMapping = getEnv("TABLE_MAPPING");
  const tableMappingJson = JSON.parse(mysqlTableMapping);
  const tableMapping = TableMapping.parse(tableMappingJson);

  const seenTargetTableNames = new Set<string>();

  for (const sourceTableName in tableMapping) {
    validateMysqlIdentifier(sourceTableName);

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

export const getMysqlDb = (): string => {
  const mysqlDb = getEnv("MYSQL_DB");
  validateMysqlIdentifier(mysqlDb);
  return mysqlDb;
};

const validateMysqlIdentifier = (identifier: string) => {
  // https://dev.mysql.com/doc/refman/8.0/en/identifier-length.html
  const identifierIsValid = /^[_a-zA-Z0-9]{1,63}$/.test(identifier);
  if (!identifierIsValid) {
    throw new Error(`Invalid MySQL identifier - ${identifier}`);
  }
};

export const getConnectedClient = async () => {
  const mysqlHost = getEnv("MYSQL_HOST");
  const mysqlPort = (() => {
    const str = tryGetEnv("MYSQL_PORT");
    if (str == null) {
      return 3306;
    }
    const int = strictParseInt(str);
    if (int == null) {
      throw new Error(`invalid MYSQL_PORT - ${str}`);
    }
    return int;
  })();
  const mysqlUser = getEnv("MYSQL_USER");
  const mysqlPassword = getEnv("MYSQL_PASSWORD");
  const mysqlDb = getEnv("MYSQL_DB");

  const client = new Client();

  await client.connect({
    hostname: mysqlHost,
    port: mysqlPort,
    username: mysqlUser,
    password: mysqlPassword,
    db: mysqlDb,
    poolSize: 1,
  });

  return client;
};

export const getPrimaryKeyColumns = async (
  client: Client,
  mysqlDb: string,
  tableNames: string[]
): Promise<Map<string, Map<string, PrimaryKeyColumn>>> => {
  const Result = z.array(PrimaryKeyColumn);

  // Technically according to the SQL Standard you need to also join to information_schema.table_constraints
  // to check that the constraint_type is "PRIMARY KEY", but in MySQL the constraint_name is always "primary"
  // for primary keys so this single-table query is sufficient.
  // (Though unclear if other constraints can also be named "primary" which would mess up this implementation.)
  //
  // The extra "as" aliasing is necessary because on some versions of MySQL the information schema columns
  // are uppercase.
  const resultRaw = await client.query(
    oneLine`
      select
        table_name as table_name,
        column_name as column_name,
        ordinal_position as ordinal_position
      from information_schema.key_column_usage
      where constraint_name = 'primary' and table_schema = ? and table_name in ?
      order by table_name, ordinal_position
    `,
    [mysqlDb, tableNames]
  );

  const result = Result.parse(resultRaw);

  const tables: Map<string, Map<string, PrimaryKeyColumn>> = new Map();
  for (const primaryKeyColumn of result) {
    let columns = tables.get(primaryKeyColumn.table_name);
    if (columns == null) {
      columns = new Map();
      tables.set(primaryKeyColumn.table_name, columns);
    }
    columns.set(primaryKeyColumn.column_name, primaryKeyColumn);
  }

  return tables;
};

export const getColumns = async (
  client: Client,
  mysqlDb: string,
  tableNames: string[]
): Promise<Map<string, Map<string, Column>>> => {
  const Result = z.array(Column);

  // The extra "as" aliasing is necessary because on some versions of MySQL the information schema columns
  // are uppercase.
  const resultRaw = await client.query(
    oneLine`
      select
        table_name as table_name,
        column_name as column_name,
        ordinal_position as ordinal_position,
        data_type as data_type,
        is_nullable as is_nullable
      from information_schema.columns
      where table_schema = ? and table_name in ?
      order by table_name, ordinal_position
    `,
    [mysqlDb, tableNames]
  );

  const result = Result.parse(resultRaw);

  const tables: Map<string, Map<string, Column>> = new Map();
  for (const column of result) {
    let columns = tables.get(column.table_name);
    if (columns == null) {
      columns = new Map();
      tables.set(column.table_name, columns);
    }
    columns.set(column.column_name, column);
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

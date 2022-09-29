import { tableFromJSON, tableToIPC } from "@apache-arrow/es2015-esm";
import {
  DeleteRows,
  InsertRows,
  Mutation,
  Scalar,
  SyncTable,
  Transaction,
  UpdateRows,
  Uuid,
  getCatalogSnapshot,
  getEnv,
  invariant,
  querySqlSnapshot,
  registerCronHandler,
  registerTransactionHandler,
  strictParseInt,
  syncTables,
  tryGetEnv,
  unpackRows,
} from "@dataland-io/dataland-sdk-worker";
import {
  DatabaseSchema,
  transitionDatabaseSchema_mutations,
} from "@dataland-io/dataland-state-sync";
import { Client } from "@dataland-workerlibs/mysql";
import { oneLine } from "common-tags";
import { groupBy, keyBy } from "lodash-es";
import { z } from "zod";

const CRON_SYNC_MARKER = "mysql-sync.workers.dataland.io/cron-sync-marker";

const TableMapping = z.record(z.string());

type TableMapping = z.infer<typeof TableMapping>;

const getConnectedClient = async (): Promise<Client> => {
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

const getTableMapping = (): TableMapping => {
  const mysqlTableMapping = getEnv("MYSQL_TABLE_MAPPING");
  const tableMappingJson = JSON.parse(mysqlTableMapping);
  const tableMapping = TableMapping.parse(tableMappingJson);

  // validate one-to-one (multiple source tables syncing to the same target table would be bad)
  const values = new Set<string>();
  for (const sourceTable in tableMapping) {
    const targetTable = tableMapping[sourceTable];
    if (values.has(targetTable)) {
      throw new Error(
        `invalid table mapping - duplicate target table ${targetTable}`
      );
    }
    values.add(targetTable);
  }

  return tableMapping;
};

const PrimaryKeyColumn = z.object({
  table_name: z.string(),
  column_name: z.string(),
  ordinal_position: z.number(),
});
type PrimaryKeyColumn = z.infer<typeof PrimaryKeyColumn>;

const getPrimaryKeyColumns = async (
  client: Client,
  db: string,
  tables: string[]
): Promise<Record<string, PrimaryKeyColumn[]>> => {
  const Result = z.array(PrimaryKeyColumn);
  // Technically according to the SQL Standard you need to also join to information_schema.table_constraints
  // to check that the constraint_type is "PRIMARY KEY", but in MySQL the constraint_name is always "primary"
  // for primary keys so this single-table query is sufficient.
  // (Though unclear if other constraints can also be named "primary" which would mess up this implementation.)
  const resultRaw = await client.query(
    oneLine`
      select table_name as table_name, column_name as column_name, ordinal_position as ordinal_position
      from information_schema.key_column_usage
      where constraint_name = 'primary' and table_schema = ? and table_name in ?
      order by table_name, ordinal_position
    `,
    [db, tables]
  );

  const result = Result.parse(resultRaw);
  const groupByTable = groupBy(result, (c) => c.table_name);
  return groupByTable;
};

const Column = z.object({
  table_name: z.string(),
  column_name: z.string(),
  ordinal_position: z.number(),
  data_type: z.string(),
  is_nullable: z.string(),
});
type Column = z.infer<typeof Column>;

const getColumns = async (
  client: Client,
  db: string,
  tables: string[]
): Promise<Record<string, Column[]>> => {
  const Result = z.array(Column);
  const resultRaw = await client.query(
    oneLine`
      select table_name as table_name, column_name as column_name, ordinal_position as ordinal_position, data_type as data_type, is_nullable as is_nullable
      from information_schema.columns
      where table_schema = ? and table_name in ?
      order by table_name, ordinal_position
    `,
    [db, tables]
  );
  const result = Result.parse(resultRaw);
  const groupByTable = groupBy(result, (c) => c.table_name);
  return groupByTable;
};

const ingestTable = async (
  client: Client,
  sourceTable: string,
  targetTable: string,
  primaryKeyColumns: PrimaryKeyColumn[]
) => {
  const Result = z.array(z.record(z.unknown()));
  const resultRaw = await client.query("select * from ??", [sourceTable]);
  const result = Result.parse(resultRaw);

  if (result.length === 0) {
    console.log("skipping empty table", sourceTable);
    return;
  }

  postprocessRows(result);
  const arrowTable = tableFromJSON(result);
  const arrowRecordBatch = tableToIPC(arrowTable);
  const syncTable: SyncTable = {
    tableName: targetTable,
    arrowRecordBatches: [arrowRecordBatch],
    identityColumnNames: primaryKeyColumns.map((c) => c.column_name),
  };

  await syncTables({
    syncTables: [syncTable],
    transactionAnnotations: {
      [CRON_SYNC_MARKER]: "true",
    },
  });
};

const postprocessRows = (rows: Record<string, unknown>[]) => {
  for (const row of rows) {
    for (const key in row) {
      const value = row[key];
      if (
        typeof value === "string" ||
        typeof value === "number" ||
        typeof value === "boolean"
      ) {
        // continue
      } else if (value == null) {
        row[key] = null;
        // continue
      } else {
        row[key] = String(value);
      }
    }
  }
};

const cronHandler = async () => {
  const tableMapping = getTableMapping();
  const db = getEnv("MYSQL_DB");
  const client = await getConnectedClient();

  try {
    const primaryKeys = await getPrimaryKeyColumns(
      client,
      db,
      Object.keys(tableMapping)
    );

    for (const sourceTable in tableMapping) {
      const targetTable = tableMapping[sourceTable];
      const primaryKey = primaryKeys[sourceTable];
      if (primaryKey == null) {
        console.warn(
          "table either does not exist or does not have a primary key - skipping",
          sourceTable
        );
        continue;
      }
      console.log(
        `ingesting table from source "${sourceTable}" to target "${targetTable}"`
      );
      try {
        await ingestTable(client, sourceTable, targetTable, primaryKey);
        console.log(
          `successfully ingested table from source "${sourceTable}" to target "${targetTable}"`
        );
      } catch (e) {
        console.error("failed to ingest table", sourceTable, e);
        // continue to next table
      }
    }
  } finally {
    await client.close();
  }
};

registerCronHandler(cronHandler);

const processMutations = (
  databaseSchema: DatabaseSchema,
  transaction: Transaction,
  processFn: (databaseSchema: DatabaseSchema, mutation: Mutation) => void
): string[] => {
  let currentDatabaseSchema = databaseSchema;
  const statements: string[] = [];
  for (const mutation of transaction.mutations) {
    processFn(currentDatabaseSchema, mutation);
    // Updating this copy of the schema is important because it factors into generating the next SQL statement.
    // For example, suppose in one transaction a column is renamed from A -> B -> C.
    // If we don't update this copy, then when we encounter the "rename to C",
    // we'd generate "rename A to C", which would be incorrect and rejected by Postgres.
    // The correct statement would be "rename B to C", which requires us to perform A -> B on this copy.
    currentDatabaseSchema = transitionDatabaseSchema_mutations(
      currentDatabaseSchema,
      [mutation]
    );
  }
  return statements;
};

const processMutationsAsync = async (
  databaseSchema: DatabaseSchema,
  transaction: Transaction,
  processFn: (
    databaseSchema: DatabaseSchema,
    mutation: Mutation
  ) => Promise<void>
): Promise<string[]> => {
  let currentDatabaseSchema = databaseSchema;
  const statements: string[] = [];
  for (const mutation of transaction.mutations) {
    await processFn(currentDatabaseSchema, mutation);
    // Updating this copy of the schema is important because it factors into generating the next SQL statement.
    // For example, suppose in one transaction a column is renamed from A -> B -> C.
    // If we don't update this copy, then when we encounter the "rename to C",
    // we'd generate "rename A to C", which would be incorrect and rejected by Postgres.
    // The correct statement would be "rename B to C", which requires us to perform A -> B on this copy.
    currentDatabaseSchema = transitionDatabaseSchema_mutations(
      currentDatabaseSchema,
      [mutation]
    );
  }
  return statements;
};

const getMentionedSourceTables = (
  reverseTableMapping: Record<string, string>,
  databaseSchema: DatabaseSchema,
  transaction: Transaction
): string[] => {
  const ret: string[] = [];
  processMutations(databaseSchema, transaction, (databaseSchema, mutation) => {
    if (
      mutation.kind === "insert_rows" ||
      mutation.kind === "update_rows" ||
      mutation.kind === "delete_rows"
    ) {
      const sourceTable = getSourceTable(
        reverseTableMapping,
        databaseSchema,
        mutation.value.tableUuid
      );
      if (sourceTable != null) {
        ret.push(sourceTable);
      }
    }
  });
  return ret;
};

const transactionToSqlStatements = async (
  reverseTableMapping: Record<string, string>,
  databaseSchema: DatabaseSchema,
  pkColumnsLookup: Record<string, PrimaryKeyColumn[]>,
  columnsLookup: Record<string, Column[]>,
  transaction: Transaction
): Promise<Array<{ sql: string; params: any[] }>> => {
  const statements: Array<{ sql: string; params: any[] }> = [];

  await processMutationsAsync(
    databaseSchema,
    transaction,
    async (databaseSchema, mutation) => {
      if (mutation.kind === "insert_rows") {
        const { tableUuid, columnMapping, rows }: InsertRows = mutation.value;
        const tableDescriptor = databaseSchema[tableUuid];
        if (tableDescriptor == null) {
          console.warn(
            "Could not find table descriptor for table in transaction",
            tableUuid
          );
          return;
        }
        const targetTable = tableDescriptor.tableName;
        const sourceTable = reverseTableMapping[targetTable];
        if (sourceTable == null) {
          return;
        }
        const sourcePkColumns = pkColumnsLookup[sourceTable];
        const sourceColumns = columnsLookup[sourceTable];
        if (sourcePkColumns == null || sourceColumns == null) {
          return;
        }
        const sourceColumnSet = new Set<string>();
        for (const sourceColumn of sourceColumns) {
          sourceColumnSet.add(sourceColumn.column_name);
        }
        const relevantColumnIndices: number[] = [];
        const relevantColumns: string[] = [];
        const columnDescriptors = keyBy(
          tableDescriptor.columnDescriptors,
          (c) => c.columnUuid
        );
        for (let i = 0; i < columnMapping.length; i++) {
          const columnUuid = columnMapping[i];
          const columnName = columnDescriptors[columnUuid].columnName;
          if (sourceColumnSet.has(columnName)) {
            relevantColumnIndices.push(i);
            relevantColumns.push(columnName);
          }
        }

        // https://dev.mysql.com/doc/refman/8.0/en/insert.html
        const columnList = relevantColumns.map(() => "??").join(", ");
        const valueList = relevantColumns.map(() => "?").join(", ");
        // TODO(hzuo): Add `returning *` and generate a Dataland transaction to immediately
        //  display the defaulted values (e.g. auto-incrementing primary keys).
        // TODO(hzuo): Start the transaction outside, and let this code buffer up "writeback mutations"
        const sql = `insert into ?? (${columnList}) values (${valueList})`;

        for (const row of rows) {
          const params: any[] = [sourceTable];
          for (const relevantColumn of relevantColumns) {
            params.push(relevantColumn);
          }
          for (const index of relevantColumnIndices) {
            const value = row.values[index];
            params.push(value?.value ?? null);
          }
          statements.push({ sql, params });
        }
      } else if (mutation.kind === "update_rows") {
        const { tableUuid, columnMapping, rows }: UpdateRows = mutation.value;
        const tableDescriptor = databaseSchema[tableUuid];
        if (tableDescriptor == null) {
          console.warn(
            "Could not find table descriptor for table in transaction",
            tableUuid
          );
          return;
        }
        const targetTable = tableDescriptor.tableName;
        const sourceTable = reverseTableMapping[targetTable];
        if (sourceTable == null) {
          return;
        }
        const sourcePkColumns = pkColumnsLookup[sourceTable];
        const sourceColumns = columnsLookup[sourceTable];
        if (sourcePkColumns == null || sourceColumns == null) {
          return;
        }
        const sourceColumnSet = new Set<string>();
        for (const sourceColumn of sourceColumns) {
          sourceColumnSet.add(sourceColumn.column_name);
        }
        const relevantColumns: string[] = [];
        const relevantColumnIndices: number[] = [];
        const columnDescriptors = keyBy(
          tableDescriptor.columnDescriptors,
          (c) => c.columnUuid
        );
        for (let i = 0; i < columnMapping.length; i++) {
          const columnUuid = columnMapping[i];
          const columnName = columnDescriptors[columnUuid].columnName;
          if (sourceColumnSet.has(columnName)) {
            relevantColumns.push(columnName);
            relevantColumnIndices.push(i);
          }
        }
        if (relevantColumns.length === 0) {
          // not updating any source columns
          return;
        }

        // grab primary keys
        const pkNames = sourcePkColumns
          .map((k) => `"${k.column_name}"`)
          .join(", ");
        const keys = rows.map((r) => r.key).join(", ");
        const sqlQuery = `select _dataland_key, ${pkNames} from "${targetTable}" where _dataland_key in (${keys})`;
        const response = await querySqlSnapshot({
          sqlQuery,
          // TODO(hzuo): This isn't quite right
          logicalTimestamp: transaction.logicalTimestamp - 1,
        });
        const pkRows = unpackRows(response);

        const pkParamsMap = getPkParamsMap(sourcePkColumns, pkRows);

        if (pkParamsMap.size === 0) {
          return;
        }

        // https://dev.mysql.com/doc/refman/8.0/en/update.html
        const assignmentList = relevantColumns.map(() => "?? = ?").join(", ");
        const conditionList = sourcePkColumns.map(() => "?? = ?").join(", ");
        const sql = `update ?? set ${assignmentList} where ${conditionList}`;

        for (const row of rows) {
          const pkParams = pkParamsMap.get(row.key);
          if (pkParams == null) {
            console.warn(
              "Skipping row update due to missing source primary key",
              {
                _dataland_key: row.key,
              }
            );
            continue;
          }
          const params: any[] = [sourceTable];
          for (let i = 0; i < relevantColumns.length; i++) {
            const relevantColumn = relevantColumns[i];
            const relevantColumnIndex = relevantColumnIndices[i];

            params.push(relevantColumn);

            const value = row.values[relevantColumnIndex];
            params.push(value?.value ?? null);
          }
          for (const pkParam of pkParams) {
            params.push(pkParam);
          }
          statements.push({ sql, params });
        }
      } else if (mutation.kind === "delete_rows") {
        const { tableUuid, keys }: DeleteRows = mutation.value;
        const tableDescriptor = databaseSchema[tableUuid];
        if (tableDescriptor == null) {
          console.warn(
            "Could not find table descriptor for table in transaction",
            tableUuid
          );
          return;
        }
        const targetTable = tableDescriptor.tableName;
        const sourceTable = reverseTableMapping[targetTable];
        if (sourceTable == null) {
          return;
        }
        const sourcePkColumns = pkColumnsLookup[sourceTable];
        if (sourcePkColumns == null) {
          return;
        }

        const pkNames = sourcePkColumns
          .map((k) => `"${k.column_name}"`)
          .join(", ");
        const keyList = keys.join(",");
        const sqlQuery = `select _dataland_key, ${pkNames} from "${targetTable}" where _dataland_key in (${keyList})`;
        const response = await querySqlSnapshot({
          sqlQuery,
          logicalTimestamp: transaction.logicalTimestamp - 1,
        });
        const pkRows = unpackRows(response);

        const pkParamsMap = getPkParamsMap(sourcePkColumns, pkRows);

        if (pkParamsMap.size === 0) {
          return;
        }

        // https://dev.mysql.com/doc/refman/8.0/en/delete.html
        const conditionList = sourcePkColumns.map(() => "?? = ?").join(", ");
        const sql = `delete from ?? where ${conditionList}`;

        for (const key of keys) {
          const pkParams = pkParamsMap.get(key);
          if (pkParams == null) {
            console.warn(
              "Skipping row delete due to missing source primary key",
              {
                _dataland_key: key,
              }
            );
            continue;
          }
          const params: any[] = [sourceTable];
          for (const pkParam of pkParams) {
            params.push(pkParam);
          }
          statements.push({ sql, params });
        }
      }
    }
  );

  return statements;
};

const getPkParamsMap = (
  sourcePkColumns: PrimaryKeyColumn[],
  pkRows: Record<string, Scalar>[]
): Map<number, Scalar[]> => {
  const pkParamsMap = new Map<number, Scalar[]>();
  for (const pkRow of pkRows) {
    const key = pkRow["_dataland_key"];
    invariant(key != null);
    invariant(typeof key === "number");
    const pkParams: Scalar[] = [];
    let missingPk = false;
    for (const pkColumn of sourcePkColumns) {
      const pkName = pkColumn.column_name;
      const pkValue = pkRow[pkName];
      if (pkValue == null) {
        missingPk = true;
        break;
      }
      pkParams.push(pkName);
      pkParams.push(pkRow[pkName]);
    }
    if (missingPk) {
      console.warn("Source primary key is missing for row", {
        _dataland_key: key,
      });
    } else {
      pkParamsMap.set(key, pkParams);
    }
  }
  return pkParamsMap;
};

const getReverseTableMapping = (): Record<string, string> => {
  const tableMapping = getTableMapping();
  const reverseTableMapping: Record<string, string> = {};
  for (const sourceTable in tableMapping) {
    const targetTable = tableMapping[sourceTable];
    reverseTableMapping[targetTable] = sourceTable;
  }
  return reverseTableMapping;
};

const getSourceTable = (
  reverseTableMapping: Record<string, string>,
  databaseSchema: DatabaseSchema,
  tableUuid: Uuid
): string | null => {
  const tableDescriptor = databaseSchema[tableUuid];
  if (tableDescriptor == null) {
    return null;
  }
  const targetTable = tableDescriptor.tableName;
  const sourceTable = reverseTableMapping[targetTable] ?? null;
  return sourceTable;
};

const transactionHandler = async (transaction: Transaction) => {
  const reverseTableMapping = getReverseTableMapping();
  const db = getEnv("MYSQL_DB");

  if (CRON_SYNC_MARKER in transaction.transactionAnnotations) {
    console.log("skipping own cron sync transactions from writeback");
  }

  const { tableDescriptors } = await getCatalogSnapshot({
    // Note that passing `transaction.logicalTimestamp - 1` means that we're
    // reading state of the schema at the instant **before** this transaction happened.
    logicalTimestamp: transaction.logicalTimestamp - 1,
  });
  const databaseSchema: DatabaseSchema = keyBy(
    tableDescriptors,
    (x) => x.tableUuid
  );

  const mentionedSourceTables = getMentionedSourceTables(
    reverseTableMapping,
    databaseSchema,
    transaction
  );

  if (mentionedSourceTables.length === 0) {
    return;
  }

  const client = await getConnectedClient();

  try {
    const primaryKeyColumns = await getPrimaryKeyColumns(
      client,
      db,
      mentionedSourceTables
    );
    const columns = await getColumns(client, db, mentionedSourceTables);

    const sqlStatements = await transactionToSqlStatements(
      reverseTableMapping,
      databaseSchema,
      primaryKeyColumns,
      columns,
      transaction
    );

    if (sqlStatements.length === 0) {
      return;
    }

    const transactionUuid = transaction.transactionUuid;

    console.log(
      "replicating transaction",
      transactionUuid,
      sqlStatements.length
    );

    await client.transaction(async (connection) => {
      for (const sqlStatement of sqlStatements) {
        console.log("running statement", transactionUuid, sqlStatement);
        const result = await connection.execute(
          sqlStatement.sql,
          sqlStatement.params
        );
        console.log("completed statement", transactionUuid, result);
      }
    });

    console.log("completed transaction", transactionUuid);
  } finally {
    client.close();
  }
};

registerTransactionHandler(transactionHandler);

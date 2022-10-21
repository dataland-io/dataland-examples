import {
  AddColumn,
  assertNever,
  ChangeColumnNullable,
  ColumnDescriptor,
  CreateTable,
  DataType,
  DeleteRows,
  DropColumn,
  DropTable,
  getCatalogSnapshot,
  getEnv,
  InsertRows,
  invariant,
  Mutation,
  registerTransactionHandler,
  RenameColumn,
  RenameTable,
  strictParseInt,
  TaggedScalar,
  Transaction,
  tryGetEnv,
  UpdateRows,
} from "@dataland-io/dataland-sdk";
import {
  DatabaseSchema,
  transitionDatabaseSchema_mutations,
} from "@dataland-io/dataland-state-sync";
import { Client } from "@dataland-workerlibs/postgres";
import { keyBy } from "lodash-es";

const handler = async (transaction: Transaction) => {
  const { tableDescriptors } = await getCatalogSnapshot({
    // Note that passing `transaction.logicalTimestamp - 1` means that we're
    // reading state of the schema at the instant **before** this transaction happened.
    logicalTimestamp: transaction.logicalTimestamp - 1,
  });
  const databaseSchema: DatabaseSchema = keyBy(
    tableDescriptors,
    (x) => x.tableUuid
  );

  const sqlStatements = transactionToSqlStatements(databaseSchema, transaction);

  if (sqlStatements.length === 0) {
    return;
  }

  const transactionUuid = transaction.transactionUuid;

  console.log("replicating transaction", transactionUuid, sqlStatements.length);

  const pgHost = getEnv("PGHOST");
  const pgPort = (() => {
    const str = tryGetEnv("PGPORT");
    if (str == null) {
      return 6543;
    }
    const int = strictParseInt(str);
    if (int == null) {
      throw new Error(`invalid PGPORT - ${str}`);
    }
    return int;
  })();
  const pgDatabase = tryGetEnv("PGDATABASE") ?? "postgres";
  const pgUser = tryGetEnv("PGUSER") ?? "postgres";
  const pgPassword = getEnv("PGPASSWORD");
  const pgSchema = tryGetEnv("PGSCHEMA") ?? "public";

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
  await client.connect();

  console.log("connected to database", transactionUuid);

  const tx = await client.createTransaction(transaction.transactionUuid, {
    isolation_level: "serializable",
    read_only: false,
  });

  await tx.begin();

  console.log("started tx", transactionUuid);

  await tx.queryArray(
    `create schema if not exists ${quoteIdentifier(pgSchema)}`
  );
  await tx.queryArray(`set local search_path to ${quoteIdentifier(pgSchema)}`);

  for (const sqlStatement of sqlStatements) {
    console.log("running statement", transactionUuid, sqlStatement);
    const result = await tx.queryArray(sqlStatement);
    console.log("completed statement", transactionUuid, result);
  }

  await tx.commit();

  console.log("committed tx", transactionUuid);

  await client.end();

  console.log("disconnected from database", transactionUuid);
};

registerTransactionHandler(handler, {
  filterTransactions: "handle-all",
});

export const transactionToSqlStatements = (
  databaseSchema: DatabaseSchema,
  transaction: Transaction
): string[] => {
  let currentDatabaseSchema = databaseSchema;
  const statements: string[] = [];
  for (const mutation of transaction.mutations) {
    const statements2 = mutationToSqlStatements(
      currentDatabaseSchema,
      mutation
    );
    for (const statement of statements2) {
      statements.push(statement);
    }
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

export const quoteIdentifier = (identifier: string): string => {
  return '"' + identifier.replaceAll('"', '""') + '"';
};

export const quoteConstant = (constant: string): string => {
  return "'" + constant.replaceAll("'", "''") + "'";
};

export const quoteTaggedScalar = (taggedScalar: TaggedScalar): string => {
  if (taggedScalar == null) {
    return "null";
  }
  switch (taggedScalar.kind) {
    case "boolean":
    case "int32":
    case "int64":
    case "float32":
    case "float64":
      return String(taggedScalar.value);
    case "string": {
      return quoteConstant(taggedScalar.value);
    }
    case "bytes": {
      invariant(!taggedScalar.value.includes("'"));
      invariant(!taggedScalar.value.includes('"'));
      return `decode('${taggedScalar.value}', 'base64')`;
    }
    default: {
      assertNever(taggedScalar);
    }
  }
};

export const getPostgresDataType = (dataType: DataType): string => {
  switch (dataType) {
    case "boolean": {
      return "boolean";
    }
    case "int32": {
      return "integer";
    }
    case "int64": {
      return "bigint";
    }
    case "float32": {
      return "real";
    }
    case "float64": {
      return "double precision";
    }
    case "string": {
      return "text";
    }
    case "bytes": {
      return "bytea";
    }
    default: {
      assertNever(dataType);
    }
  }
};

export const getPostgresColumnInfo = (
  columnDescriptor: ColumnDescriptor
): string => {
  switch (columnDescriptor.columnName) {
    case "_dataland_key": {
      return `_dataland_key bigint primary key`;
    }
    case "_dataland_ordinal": {
      return `_dataland_ordinal text collate "C" not null unique`;
    }
    default: {
      const components = [];
      components.push(quoteIdentifier(columnDescriptor.columnName));
      components.push(getPostgresDataType(columnDescriptor.dataType));
      if (columnDescriptor.dataType === "string") {
        components.push(`collate "C"`);
      }
      if (!columnDescriptor.nullable) {
        components.push("not null");
      }
      return components.join(" ");
    }
  }
};

export const mutationToSqlStatements = (
  databaseSchema: DatabaseSchema,
  mutation: Mutation
): string[] => {
  // TODO(hzuo): Add Schema constructor that takes a DatabaseSchema
  switch (mutation.kind) {
    case "insert_rows": {
      const insertRows: InsertRows = mutation.value;
      const tableDescriptor = databaseSchema[insertRows.tableUuid];
      invariant(tableDescriptor != null);
      const tableName = quoteIdentifier(tableDescriptor.tableName);
      const columnDescriptors = keyBy(
        tableDescriptor.columnDescriptors,
        (c) => c.columnUuid
      );
      const columnNames = insertRows.columnMapping.map((columnUuid) => {
        const columnDescriptor = columnDescriptors[columnUuid];
        invariant(columnDescriptor != null);
        return quoteIdentifier(columnDescriptor.columnName);
      });
      const insertStatements = insertRows.rows.map((row) => {
        const valueList: string[] = [String(row.key)];
        invariant(row.values.length === columnNames.length);
        for (const taggedScalar of row.values) {
          // TODO(hzuo): This is potentially a lossy string conversion if taggedScalar is a float
          valueList.push(quoteTaggedScalar(taggedScalar));
        }
        // prettier-ignore
        return `insert into ${tableName} (_dataland_key, ${columnNames.join(",")}) values (${valueList.join(",")})`;
      });
      return insertStatements;
    }
    case "update_rows": {
      const updateRows: UpdateRows = mutation.value;
      const tableDescriptor = databaseSchema[updateRows.tableUuid];
      invariant(tableDescriptor != null);
      const tableName = quoteIdentifier(tableDescriptor.tableName);
      const columnDescriptors = keyBy(
        tableDescriptor.columnDescriptors,
        (c) => c.columnUuid
      );
      const columnNames = updateRows.columnMapping.map((columnUuid) => {
        const columnDescriptor = columnDescriptors[columnUuid];
        invariant(columnDescriptor != null);
        return quoteIdentifier(columnDescriptor.columnName);
      });
      const updateStatements = updateRows.rows.map((row) => {
        const setClauses: string[] = [];
        invariant(row.values.length === columnNames.length);
        for (let i = 0; i < row.values.length; i++) {
          const columnName = columnNames[i]!;
          const taggedScalar = row.values[i]!;
          const value = quoteTaggedScalar(taggedScalar);
          const setClause = `${columnName} = ${value}`;
          setClauses.push(setClause);
        }
        // prettier-ignore
        return `update ${tableName} set ${setClauses.join(",")} where _dataland_key = ${row.key}`;
      });
      return updateStatements;
    }
    case "delete_rows": {
      const deleteRows: DeleteRows = mutation.value;
      const tableDescriptor = databaseSchema[deleteRows.tableUuid];
      invariant(tableDescriptor != null);
      const tableName = quoteIdentifier(tableDescriptor.tableName);
      const deleteStatements = deleteRows.keys.map((key) => {
        return `delete from ${tableName} where _dataland_key = ${key}`;
      });
      return deleteStatements;
    }
    case "add_column": {
      const addColumn: AddColumn = mutation.value;
      const columnDescriptor = addColumn.columnDescriptor;
      const tableDescriptor = databaseSchema[columnDescriptor.tableUuid];
      invariant(tableDescriptor != null);
      const tableName = quoteIdentifier(tableDescriptor.tableName);
      const columnInfo = getPostgresColumnInfo(columnDescriptor);
      return [`alter table ${tableName} add column ${columnInfo}`];
    }
    case "drop_column": {
      const dropColumn: DropColumn = mutation.value;
      const tableDescriptor = databaseSchema[dropColumn.tableUuid];
      invariant(tableDescriptor != null);
      const columnDescriptor = tableDescriptor.columnDescriptors.find(
        (c) => c.columnUuid === dropColumn.columnUuid
      );
      invariant(columnDescriptor != null);
      const tableName = quoteIdentifier(tableDescriptor.tableName);
      const columnName = quoteIdentifier(columnDescriptor.columnName);
      return [`alter table ${tableName} drop column ${columnName}`];
    }
    case "rename_column": {
      const renameColumn: RenameColumn = mutation.value;
      const tableDescriptor = databaseSchema[renameColumn.tableUuid];
      invariant(tableDescriptor != null);
      const columnDescriptor = tableDescriptor.columnDescriptors.find(
        (c) => c.columnUuid === renameColumn.columnUuid
      );
      invariant(columnDescriptor != null);
      const tableName = quoteIdentifier(tableDescriptor.tableName);
      const columnName = quoteIdentifier(columnDescriptor.columnName);
      const newColumnName = quoteIdentifier(renameColumn.columnName);
      return [
        `alter table ${tableName} rename column ${columnName} to ${newColumnName}`,
      ];
    }
    case "change_column_nullable": {
      const changeColumnNullable: ChangeColumnNullable = mutation.value;
      const tableDescriptor = databaseSchema[changeColumnNullable.tableUuid];
      invariant(tableDescriptor != null);
      const columnDescriptor = tableDescriptor.columnDescriptors.find(
        (c) => c.columnUuid === changeColumnNullable.columnUuid
      );
      invariant(columnDescriptor != null);
      const tableName = quoteIdentifier(tableDescriptor.tableName);
      const columnName = quoteIdentifier(columnDescriptor.columnName);
      if (changeColumnNullable.nullable) {
        return [
          `alter table ${tableName} alter column ${columnName} drop not null`,
        ];
      } else {
        return [
          `alter table ${tableName} alter column ${columnName} set not null`,
        ];
      }
    }
    case "reorder_column": {
      return [];
    }
    case "set_column_annotation": {
      return [];
    }
    case "create_table": {
      const createTable: CreateTable = mutation.value;
      const tableDescriptor = createTable.tableDescriptor;
      const tableName = quoteIdentifier(tableDescriptor.tableName);
      const columnInfos = tableDescriptor.columnDescriptors.map(
        getPostgresColumnInfo
      );
      return [`create table ${tableName} (${columnInfos.join(",")})`];
    }
    case "drop_table": {
      const dropTable: DropTable = mutation.value;
      const tableDescriptor = databaseSchema[dropTable.tableUuid];
      invariant(tableDescriptor != null);
      const tableName = quoteIdentifier(tableDescriptor.tableName);
      return [`drop table ${tableName}`];
    }
    case "rename_table": {
      const renameTable: RenameTable = mutation.value;
      const tableDescriptor = databaseSchema[renameTable.tableUuid];
      invariant(tableDescriptor != null);
      const tableName = quoteIdentifier(tableDescriptor.tableName);
      const newTableName = quoteIdentifier(renameTable.tableName);
      return [`alter table ${tableName} rename to ${newTableName}`];
    }
    case "set_table_annotation": {
      return [];
    }
    default: {
      assertNever(mutation);
    }
  }
};

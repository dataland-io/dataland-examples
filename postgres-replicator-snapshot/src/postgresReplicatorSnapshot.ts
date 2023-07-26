import {
  ColumnDescriptor,
  CronEvent,
  DataType,
  ResultRow,
  Scalar,
  TableDescriptor,
  assertNever,
  getDbClient,
  getEnv,
  registerCronHandler,
  strictParseInt,
  tryGetEnv,
  unpackRows,
} from "@dataland-io/dataland-sdk";
import { Client } from "@dataland-workerlibs/postgres";

const db = getDbClient();

registerCronHandler(async (_: CronEvent) => {
  const t1 = performance.now();
  const getCatalogResponse = await db.getCatalog({}).response;

  const pgSchema = getPgSchema();

  // TODO(hzuo): Don't re-connect for each new transaction
  const client = getClient();
  await client.connect();

  try {
    for (const tableDescriptor of getCatalogResponse.tableDescriptors) {
      const t2 = performance.now();
      const rows = await loadTable(tableDescriptor.tableName);
      const t3 = performance.now();

      const sqlStatements = rowsToSqlStatements(
        tableDescriptor,
        rows,
        pgSchema
      );

      if (sqlStatements.length === 0) {
        return;
      }

      console.log("Starting to copy table", {
        tableName: tableDescriptor.tableName,
      });

      const tx = client.createTransaction(`${tableDescriptor.tableName}`);
      await tx.begin();
      for (const sqlStatement of sqlStatements) {
        try {
          await tx.queryObject(sqlStatement);
        } catch (error) {
          throw new Error(
            `Writeback failed - query <> ${JSON.stringify(error)}`,
            { cause: error }
          );
        }
      }
      await tx.commit();

      const t4 = performance.now();

      console.log("Completed copying table", {
        tableName: tableDescriptor.tableName,
        timeToFetchData: t3 - t2,
        timeToWriteData: t4 - t2,
      });
    }
  } finally {
    await client.end();
  }
  console.log("Finished Cron run", { elapsed: performance.now() - t1 });
});

export const loadTable = async (tableName: string): Promise<ResultRow[]> => {
  const arrowRecordBatches: Uint8Array[] = [];
  const arrowLoadTableResponse = db.arrowLoadTable({ tableName });

  for await (const response0 of arrowLoadTableResponse.responses) {
    const response = response0.kind;
    if (response.oneofKind == null) {
      continue;
    }
    if (response.oneofKind === "start") {
      // handle start
    } else if (response.oneofKind === "data") {
      // handle data
      arrowRecordBatches.push(...response.data.arrowRecordBatches);
    } else if (response.oneofKind === "finish") {
      // handle finish
    } else {
      assertNever(response);
    }
  }

  return unpackRows({ arrowRecordBatches });
};

export const rowsToSqlStatements = (
  tableDescriptor: TableDescriptor,
  rows: ResultRow[],
  pgSchema: string
): string[] => {
  const statements = [
    `drop table if exists ${pgSchema}.${tableDescriptor.tableName}`,
  ];
  const tableName = tableDescriptor.tableName;
  const columnDefinitions = tableDescriptor.columnDescriptors.map((c) =>
    getPostgresColumnDefinition(c)
  );
  const allColumnDefinitions = [
    "_row_id int8 primary key",
    ...columnDefinitions,
  ];
  // prettier-ignore
  statements.push(`create table ${pgSchema}.${tableName} (${allColumnDefinitions.join(",")})`);

  if (rows.length == 0) {
    return statements;
  }

  const allColumnNames = [
    "_row_id",
    ...tableDescriptor.columnDescriptors.map((c) => c.columnName),
  ];

  console.log(`Creating statements for table ${tableDescriptor.tableName}`);
  const rowValues: Scalar[][] = [];

  for (const row of rows) {
    const values: Scalar[] = [];

    for (const column of allColumnNames) {
      let value = row[column];
      if (typeof value == "string") {
        // if (value.includes("for")) {
        //   console.log("WARNING: String contains for", { value });
        // }
        // Note that SQL needs us to escape single quotes by providing two of them.
        value = `'${value.replaceAll("'", "''")}'`;
      }
      if (value == null) {
        value = "null";
      }
      if (typeof value == "number") {
        if (`${value}`.includes("NaN")) {
          value = "null";
        }
      }
      values.push(value);
    }

    rowValues.push(values);
  }

  const rowValuesRaw = rowValues.map((r) => `(${r.join(",")})`).join(", ");

  statements.push(
    `insert into ${pgSchema}.${tableName} (${allColumnNames.join(
      ", "
    )}) values ${rowValuesRaw}`
  );

  return statements;
};

const getPostgresColumnDefinition = (
  columnDescriptor: ColumnDescriptor
): string => {
  const components = [];
  components.push(columnDescriptor.columnName);
  components.push(getPostgresDataType(columnDescriptor.dataType));
  if (columnDescriptor.dataType === DataType.STRING) {
    components.push(`collate "C"`);
  }
  if (!columnDescriptor.nullable) {
    components.push("not null");
  }
  return components.join(" ");
};

const getPostgresDataType = (dataType: DataType): string => {
  switch (dataType) {
    case DataType.UNKNOWN_DATA_TYPE:
      throw new Error("unknown data type");
    case DataType.BOOL:
      return "bool";
    case DataType.INT32:
      return "int4";
    case DataType.INT64:
      return "int8";
    case DataType.FLOAT32:
      return "float4";
    case DataType.FLOAT64:
      return "float8";
    case DataType.STRING:
      return "text";
    default:
      assertNever(dataType);
  }
};

export const getClient = () => {
  const pgHost = getEnv("PGRS_HOST");
  const pgPort = (() => {
    const str = tryGetEnv("PGRS_PORT");
    if (str == null) {
      return 6543;
    }
    const int = strictParseInt(str);
    if (int == null) {
      throw new Error(`invalid PGRS_PORT - ${str}`);
    }
    return int;
  })();
  const pgDatabase = tryGetEnv("PGRS_DATABASE") ?? "postgres";
  const pgUser = tryGetEnv("PGRS_USER") ?? "postgres";
  const pgPassword = getEnv("PGRS_PASSWORD");

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
      attempts: 5,
    },
  });
  return client;
};

export const getPgSchema = (): string => {
  const pgSchema = tryGetEnv("PGRS_SCHEMA");
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

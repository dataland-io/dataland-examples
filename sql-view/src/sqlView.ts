import {
  getDbClient,
  getHistoryClient,
  TableSyncRequest,
  registerCronHandler,
  getEnv,
} from "@dataland-io/dataland-sdk";

const handler = async () => {
  const db = await getDbClient();
  const history = await getHistoryClient();

  // Import parameters
  const sqlQuery = getEnv("SQL_QUERY");
  if (sqlQuery == null) {
    throw new Error("Missing environment variable - SQL_QUERY");
  }
  const sqlViewDatalandTableName = getEnv("SQL_VIEW_DATALAND_TABLE_NAME");
  if (sqlViewDatalandTableName == null) {
    throw new Error(
      "Missing environment variable - SQL_VIEW_DATALAND_TABLE_NAME"
    );
  }
  const sqlViewPrimaryKeyColumnName = getEnv(
    "SQL_VIEW_PRIMARY_KEY_COLUMN_NAME"
  );
  if (sqlViewPrimaryKeyColumnName == null) {
    throw new Error(
      "Missing environment variable - SQL_VIEW_PRIMARY_KEY_COLUMN_NAME"
    );
  }
  const exampleInputDatalandTableName = getEnv(
    "EXAMPLE_INPUT_DATALAND_TABLE_NAME"
  );
  if (exampleInputDatalandTableName == null) {
    throw new Error(
      "Missing environment variable - EXAMPLE_INPUT_DATALAND_TABLE_NAME"
    );
  }

  const sqlViewDatalandTableName_normalized = sqlViewDatalandTableName
    .toLowerCase()
    .replace(/[\s-]/g, "_")
    .replace(/[^0-9a-z_]/g, "")
    .replace(/^[^a-z]*/, "")
    .slice(0, 63);

  // Construct the new join query

  const test_query = await history.querySqlMirror({
    sqlQuery: `select * from ${exampleInputDatalandTableName} limit 1`,
  }).response;

  const last_logical_timestamp = test_query.logicalTimestamp;

  const joined_query = await history.querySqlSnapshot({
    logicalTimestamp: last_logical_timestamp,
    sqlQuery: `${sqlQuery}`,
  }).response;

  const tableSyncRequest: TableSyncRequest = {
    tableName: sqlViewDatalandTableName_normalized,
    arrowRecordBatches: joined_query.arrowRecordBatches,
    primaryKeyColumnNames: [sqlViewPrimaryKeyColumnName],
    dropExtraColumns: false,
    deleteExtraRows: true,
    transactionAnnotations: {},
    tableAnnotations: {},
    columnAnnotations: {},
  };

  await db.tableSync(tableSyncRequest);
};

registerCronHandler(handler);

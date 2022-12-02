import {
  registerCronHandler,
  getDbClient,
  getHistoryClient,
  TableSyncRequest,
} from "@dataland-io/dataland-sdk";

const handler = async () => {
  // Construct the new join query
  const db = await getDbClient();
  const history = await getHistoryClient();

  // --------------------------------------------------------
  // TODO: Replace the example query with your desired query
  // Available table and column names are 1-to-1 with what you see in the Dataland UI
  // The below example assumes that tables "users" and "orders" exist
  // --------------------------------------------------------
  const joined_query = await history.querySqlMirror({
    sqlQuery: `
      SELECT
        o.id as "order_id",
        o.user_id,
        o.total,
        u.name,
        u.email
      FROM
        "orders" o
      LEFT JOIN
        "users" u ON o.user_id = u.id
  `,
  }).response;

  if (joined_query == null) {
    return;
  }

  const tableSyncRequest: TableSyncRequest = {
    // TODO: tableName is what your query results will show up as in Dataland. Rename it here.
    tableName: "joined_table_in_dataland",
    arrowRecordBatches: joined_query.arrowRecordBatches,
    // TODO: Specify the primary key of the resulting table here. It can be a composite key.
    primaryKeyColumnNames: ["order_id"],
    dropExtraColumns: false,
    deleteExtraRows: true,
    transactionAnnotations: {},
    tableAnnotations: {},
    columnAnnotations: {},
  };

  await db.tableSync(tableSyncRequest);
};

registerCronHandler(handler);

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

  const test_query = await history.querySqlMirror({
    sqlQuery: `select * from "hubspot_companies" limit 1`,
  }).response;

  const last_logical_timestamp = test_query.logicalTimestamp;

  const joined_query = await history.querySqlSnapshot({
    logicalTimestamp: last_logical_timestamp,
    sqlQuery: `
    SELECT m.*, h.id AS hubspot_company_id FROM meeting_events_raw m
    LEFT JOIN hubspot_companies h
    ON m.company_domain = h.domain
  `,
  }).response;

  if (joined_query == null) {
    return;
  }

  const tableSyncRequest: TableSyncRequest = {
    tableName: "meeting_events_enriched",
    arrowRecordBatches: joined_query.arrowRecordBatches,
    primaryKeyColumnNames: ["record_id"],
    dropExtraColumns: false,
    deleteExtraRows: true,
    transactionAnnotations: {},
    tableAnnotations: {},
    columnAnnotations: {},
  };

  await db.tableSync(tableSyncRequest);
};

registerCronHandler(handler);

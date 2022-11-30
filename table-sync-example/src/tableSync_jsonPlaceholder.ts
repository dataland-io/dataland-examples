// Import methods from Dataland SDK
import {
  registerCronHandler,
  getDbClient,
  TableSyncRequest,
} from "@dataland-io/dataland-sdk";

// Import Apache Arrow library functions, which help transform
// JSON → Arrow which Dataland uses to store data
import { tableFromJSON, tableToIPC } from "apache-arrow";

const handler = async () => {
  // Init Dataland DB client to interact with Dataland
  const db = await getDbClient();

  // Get data from JSONPlaceholder Comments API
  const records = await getDataFromJSONPlaceholder();

  if (records == null) {
    return;
  }

  // Transform arbitrary JSON array of objects to Arrow format (Dataland's data format)
  const table = tableFromJSON(records);
  const batch = tableToIPC(table);

  // Create a TableSyncRequest to sync data to Dataland
  // This creates or updates a table in Dataland
  const tableSyncRequest: TableSyncRequest = {
    tableName: "jsonplaceholder_comments",
    arrowRecordBatches: [batch],
    primaryKeyColumnNames: ["comment_id"],
    // If you want to append columns to an existing table, set this to false
    dropExtraColumns: false,
    // If you want to append rows to an existing table, set this to false
    deleteExtraRows: true,
    transactionAnnotations: {},
    tableAnnotations: {},
    columnAnnotations: {},
  };

  await db.tableSync(tableSyncRequest);
};

const getDataFromJSONPlaceholder = async () => {
  const response = await fetch("https://jsonplaceholder.typicode.com/comments");
  const records = await response.json();

  // Reformat object keys to match Dataland's column naming conventions
  // Dataland columns can only contain lowercase [a-z], [0-9], and underscores
  const records_reformatted = records.map((record: any) => {
    return {
      comment_id: record.id,
      post_id: record.postId,
      name: record.name,
      email: record.email,
      body: record.body,
    };
  });

  return records_reformatted;
};

registerCronHandler(handler);

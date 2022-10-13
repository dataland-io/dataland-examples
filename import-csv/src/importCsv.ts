// This is a template.
// To use it, search for `TODO` and follow the steps for your use case.

import { tableFromJSON, tableToIPC } from "@apache-arrow/es2015-cjs";
import {
  TableSyncRequest,
  getDbClient,
  registerCronHandler,
} from "@dataland-io/dataland-sdk";

// TODO: First, replace the following URL with a URL that has the plain-text CSV content of your file.
// One way to do this is using Github Gists.
// See this video for instructions: https://www.loom.com/share/fb19460ff28344558d8986a362b1293c).

const CSV_URL =
  "https://gist.githubusercontent.com/arthurwuhoo/ad0b0436c5538ae63dab55c356f7a167/raw/fffb06f0528d8e586aab805c4b3098286eba69a7/customer.csv";

const fetchCSVfromCurl = async (url: string) => {
  const csv_url = url;
  const response = await fetch(csv_url);
  const data = await response.text();
  return data;
};

function csvToRowArray(csv: string) {
  const lines = csv.split("\n");
  const result = [];

  // This assumes that the CSV has column names in its first line, so it skips it (since i = 1 to start)
  for (let i = 1; i < lines.length; i++) {
    // TODO: Change the delimiter here if it's not the specified below.
    const currentline = lines[i].split("|");

    // TODO: Change the keys of this object to your column names here.
    // For example below, the first column will be stored with object key "region", and there are 14 columns total.
    const row = {
      c_custkey: currentline[0],
      c_name: currentline[1],
      c_address: currentline[2],
      c_nationkey: currentline[3],
      c_phone: currentline[4],
      c_acctbal: currentline[5],
      c_mktsegment: currentline[6],
      c_comment: currentline[7],
    };
    result.push(row);
  }
  return result;
}

// --------------------------------------------------
const handler = async () => {
  // This function will only get triggered if a user presses a button in a button column named "Trigger",
  // that's part of a table called "Import Trigger".

  // TODO: If you're using a different name for the trigger table or its column, change it here.
  const records = await fetchCSVfromCurl(CSV_URL);
  const result = csvToRowArray(records);

  const table = tableFromJSON(result);
  const batch = tableToIPC(table);

  const tableSyncRequest: TableSyncRequest = {
    tableName: "records_from_csv",
    arrowRecordBatches: [batch],
    // TODO: Your CSV must have a unique column that can be used to identify each row.
    // Put the name of that column here (instead of `c_custkey`)
    primaryKeyColumnNames: ["c_custkey"],
    dropExtraColumns: true,
    deleteExtraRows: true,
    transactionAnnotations: {},
    tableAnnotations: {},
    columnAnnotations: {},
  };

  console.log(`running TableSync - 150000 rows`);

  const db = getDbClient();
  const t0 = performance.now();
  await db.tableSync(tableSyncRequest);

  console.log(`completed TableSync - ${performance.now() - t0}ms`);
};

registerCronHandler(handler);

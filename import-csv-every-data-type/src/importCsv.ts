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
  "https://gist.githubusercontent.com/arthurwuhoo/e350c08612a22bfd28b5fa464f2c9fcf/raw/1e28b2b355108cbc8c72602f410f830895e25e79/every-data-type-2.csv";

const fetchCSVfromCurl = async (url: string) => {
  const csv_url = url;
  const response = await fetch(csv_url);
  const data = await response.text();
  return data;
};

function csvToRowArray(csv: string) {
  var lines = csv.split("\n");
  var result = [];

  // This assumes that the CSV has column names in its first line, so it skips it (since i = 1 to start)
  for (var i = 1; i < lines.length; i++) {
    // TODO: Change the delimiter here if it's not a comma.
    let currentline = lines[i].split(",");

    // TODO: Change the keys of this object to your column names here.
    // For example below, the first column will be stored with object key "region", and there are 14 columns total.
    let row = {
      id: currentline[0],
      select: currentline[1],
      button: currentline[2],
      checkbox: currentline[3],
      text: currentline[4],
      number: currentline[5],
      url: currentline[6],
      raw_string: currentline[7],
      raw_boolean: currentline[8],
      raw_int32: currentline[9],
      raw_int64: currentline[10],
      raw_float32: currentline[11],
      raw_float64: currentline[12],
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
    tableName: "every_data_type_table",
    arrowRecordBatches: [batch],
    // TODO: Your CSV must have a unique column that can be used to identify each row.
    // Put the name of that column here (instead of `id`)
    primaryKeyColumnNames: ["id"],
    dropExtraColumns: true,
    deleteExtraRows: true,
    transactionAnnotations: {},
    tableAnnotations: {},
    columnAnnotations: {},
  };

  const db = getDbClient();
  await db.tableSync(tableSyncRequest);
};

registerCronHandler(handler);

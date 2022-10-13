import { tableFromJSON, tableToIPC } from "@apache-arrow/es2015-esm";
import {
  registerCronHandler,
  getDbClient,
  TableSyncRequest,
  getEnv,
} from "@dataland-io/dataland-sdk";

import { isString } from "lodash-es";
import { ContextExclusionPlugin } from "webpack";

interface SmartsheetColumn {
  id: number;
  version: number;
  index: number;
  title: string;
  type: string;
  primary: boolean;
  validation: boolean;
  width: number;
}

const findDuplicates = (arr: Array<String>) => {
  let sorted_arr = arr.slice().sort();
  let results = [];
  for (let i = 0; i < sorted_arr.length - 1; i++) {
    if (sorted_arr[i + 1] == sorted_arr[i]) {
      results.push(sorted_arr[i]);
    }
  }
  return results;
};

const readFromSmartsheetTable = async (
  SMARTSHEET_SHEET_ID: string,
  SMARTSHEET_API_KEY: string
) => {
  const url = `https://api.smartsheet.com/2.0/sheets/${SMARTSHEET_SHEET_ID}/`;

  const options = {
    method: "GET",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${SMARTSHEET_API_KEY}`,
    },
  };

  const response = await fetch(url, options);
  const result = await response.json();

  const columns = result.columns;

  let column_titles: string[] = [];
  let primary_key_column_name = null;

  columns.forEach((column: SmartsheetColumn) => {
    const column_title_normalized = column.title
      .toLowerCase()
      .replace(/[\s-]/g, "_")
      .replace(/[^0-9a-z_]/g, "")
      .replace(/^[^a-z]*/, "")
      .slice(0, 63);

    column_titles.push(column_title_normalized);

    if (column.primary) {
      primary_key_column_name = column.title
        .toLowerCase()
        .replace(/[\s-]/g, "_")
        .replace(/[^0-9a-z_]/g, "")
        .replace(/^[^a-z]*/, "")
        .slice(0, 63);
    }
  });

  const duplicated_column_titles = findDuplicates(column_titles);

  if (duplicated_column_titles.length > 0) {
    throw new Error(
      `Duplicate normalized field names detected - ${duplicated_column_titles} - please rename fields in Smartsheet`
    );
  }

  if (primary_key_column_name == null) {
    throw new Error("No primary key column found in Smartsheet table");
  }

  const rows = result.rows;

  const records = [];
  for (const row of rows) {
    let record = {} as any;
    for (var i = 0; i < columns.length; i++) {
      const column = columns[i];
      const title = column_titles[i];
      const value = row.cells.find(
        (cell: { columnId: any }) => cell.columnId === column.id
      ).displayValue;
      record[title] = value;
    }
    records.push(record);
  }

  return [records, primary_key_column_name];
};

const cronHandler = async () => {
  const SMARTSHEET_API_KEY = getEnv("SMARTSHEET_API_KEY");

  if (SMARTSHEET_API_KEY == null) {
    throw new Error("Missing environment variable - SMARTSHEET_API_KEY");
  }

  const SMARTSHEET_SHEET_ID = getEnv("SMARTSHEET_SHEET_ID");

  if (SMARTSHEET_SHEET_ID == null) {
    throw new Error("Missing environment variable - SMARTSHEET_SHEET_ID");
  }

  const SMARTSHEET_DATALAND_TABLE_NAME = getEnv(
    "SMARTSHEET_DATALAND_TABLE_NAME"
  );

  if (SMARTSHEET_DATALAND_TABLE_NAME == null) {
    throw new Error(
      "Missing environment variable - SMARTSHEET_DATALAND_TABLE_NAME"
    );
  }
  // check if Smartsheet Dataland Table Name matches format
  if (!SMARTSHEET_DATALAND_TABLE_NAME.match(/^[a-z0-9_]+$/)) {
    throw new Error(
      "Invalid Smartsheet Dataland Table Name - must be lowercase alphanumeric and underscores only"
    );
  }

  const result = await readFromSmartsheetTable(
    SMARTSHEET_SHEET_ID,
    SMARTSHEET_API_KEY
  );

  const smartsheet_rows = result[0];
  const primary_key_column_name = result[1];

  if (!isString(primary_key_column_name)) {
    throw new Error("Primary key column name is not a string");
  }

  // console.log("smartsheet_rows: ", smartsheet_rows);

  const table = tableFromJSON(smartsheet_rows);
  const batch = tableToIPC(table);

  const tableSyncRequest: TableSyncRequest = {
    tableName: SMARTSHEET_DATALAND_TABLE_NAME,
    arrowRecordBatches: [batch],
    primaryKeyColumnNames: [primary_key_column_name],
    dropExtraColumns: false,
    deleteExtraRows: true,
    transactionAnnotations: {},
    tableAnnotations: {},
    columnAnnotations: {},
  };

  const db = getDbClient();
  await db.tableSync(tableSyncRequest);
};

registerCronHandler(cronHandler);

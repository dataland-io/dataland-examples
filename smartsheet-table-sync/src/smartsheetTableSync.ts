import { tableFromJSON, tableToIPC } from "@apache-arrow/es2015-esm";
import {
  registerCronHandler,
  Scalar,
  SyncTable,
  getEnv,
  syncTables,
} from "@dataland-io/dataland-sdk-worker";

import { isString } from "lodash-es";

const SMARTSHEET_API_KEY = getEnv("SMARTSHEET_API_KEY");

if (SMARTSHEET_API_KEY == null) {
  throw new Error("Missing environment variable - SMARTSHEET_API_KEY");
}

const SMARTSHEET_SHEET_ID = getEnv("SMARTSHEET_SHEET_ID");

if (SMARTSHEET_SHEET_ID == null) {
  throw new Error("Missing environment variable - SMARTSHEET_SHEET_ID");
}

const SMARTSHEET_DATALAND_TABLE_NAME = getEnv("SMARTSHEET_DATALAND_TABLE_NAME");

if (SMARTSHEET_DATALAND_TABLE_NAME == null) {
  throw new Error(
    "Missing environment variable - SMARTSHEET_DATALAND_TABLE_NAME"
  );
}

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

const readFromSmartsheetTable = async () => {
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

  let column_titles = [];
  let primary_key_column_name = null;

  columns.forEach((column: SmartsheetColumn) => {
    column_titles.push(column.title);
    if (column.primary) {
      primary_key_column_name = column.title;
    }
  });

  if (primary_key_column_name == null) {
    throw new Error("No primary key column found in Smartsheet table");
  }

  const rows = result.rows;

  const records = [];
  for (const row of rows) {
    let record = {} as any;
    for (const column of columns) {
      const title = column.title;
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
  const result = await readFromSmartsheetTable();
  const smartsheet_rows = result[0];
  const primary_key_column_name = result[1];

  if (!isString(primary_key_column_name)) {
    throw new Error("Primary key column name is not a string");
  }

  console.log("smartsheet_rows: ", smartsheet_rows);

  const table = tableFromJSON(smartsheet_rows);
  const batch = tableToIPC(table);

  const syncTable: SyncTable = {
    tableName: SMARTSHEET_DATALAND_TABLE_NAME,
    arrowRecordBatches: [batch],
    identityColumnNames: [primary_key_column_name],
  };

  await syncTables({
    syncTables: [syncTable],
  });
};

registerCronHandler(cronHandler);

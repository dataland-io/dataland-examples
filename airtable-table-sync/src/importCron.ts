import { tableFromJSON, tableToIPC } from "@apache-arrow/es2015-esm";
import {
  getEnv,
  registerCronHandler,
  TableSyncRequest,
  getDbClient,
  Scalar,
} from "@dataland-io/dataland-sdk";
import {
  AirtableImportedRecords,
  AirtableImportedValue,
  AIRTABLE_FIELD_NAME,
  fetchRetry,
  getDatalandTableName,
  RECORD_ID,
} from "./common";

const airtableValueToDatalandValue = (value: AirtableImportedValue): Scalar => {
  if (value == null) {
    return null;
  }
  if (
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean"
  ) {
    return value;
  }
  // NOTE(gab):
  // The reason these fields are parsed as strings is that when sending an update to Airtable,
  // Airtable expects lists to be in this format: "x,y,z" or '"x","y","z"', and not in its
  // stringified json form '["x", "y", "z"]'. This allows us to not need any additional parsing on
  // updating values. (this is only true if typecast: true is set on the update)
  //
  // Field types that NEED to be parsed as a concatenated string:
  // - Linked record (list of strings)
  // - Multiple select (list of strings)
  //
  // Additionally, all lookup fields containing only strings or numbers are also parsed as a concatenated string,
  // since the field type cannot be known beforehand. This also makes the UI more consistent.
  //
  // Full list of fields (for Dataland core team reference): https://datalandhq.quip.com/JeqZAWbt5Z9o/Module-planning-Airtable#temp:C:cZHf643296828573be85cea3bb62
  if (Array.isArray(value)) {
    const isCommaSeparated = value.every(
      (v) => typeof v === "string" || typeof v === "number"
    );
    if (isCommaSeparated === true) {
      return `"${value.join('","')}"`;
    }
    return JSON.stringify(value);
  }
  if (typeof value === "object") {
    return JSON.stringify(value);
  }

  console.error("Import - Could not parse value from Airtable", { value });
  return null;
};

const getAirtableRecords = async (): Promise<AirtableImportedRecords> => {
  const apiKey = getEnv("AIRTABLE_API_KEY");
  const baseId = getEnv("AIRTABLE_BASE_ID");
  const tableName = getEnv("AIRTABLE_TABLE_NAME");
  const viewId = getEnv("AIRTABLE_VIEW_NAME");
  const fieldListStr = getEnv("AIRTABLE_FIELDS_LIST");

  // NOTE(gab): empty array = all fields
  let fields: string[] = [];
  if (fieldListStr !== "ALL") {
    fields = fieldListStr.split(",").map((field) => field.trim());
  }
  const fieldUrlParameters = fields
    .map((field) => `fields[]=${field}`)
    .join("&");

  const records: AirtableImportedRecords = [];
  let offset = "";
  while (offset != null) {
    const url = encodeURI(
      `https://api.airtable.com/v0/${baseId}/${tableName}?offset=${offset}&pageSize=100&view=${viewId}&${fieldUrlParameters}`
    );
    const response = await fetchRetry(() =>
      fetch(url, {
        method: "GET",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${apiKey}`,
        },
      })
    );
    if (response === "error") {
      throw new Error(`Airtable import failed: ${JSON.stringify({ url })}`);
    }

    const json = await response.json();
    const importedRecords: AirtableImportedRecords = json.records;
    records.push(...importedRecords);
    offset = json.offset;
  }
  return records;
};

type ColumnName = string;
type FieldName = string;
const readRowsFromAirtable = async (): Promise<{
  rows: Record<string, Scalar>[];
  fieldNameMapping: Record<ColumnName, FieldName>;
}> => {
  const records = await getAirtableRecords();

  const columnNameMapping: Record<FieldName, ColumnName> = {};
  const fieldNameMapping: Record<ColumnName, FieldName> = {};
  for (const record of records) {
    for (const fieldName in record.fields) {
      if (fieldName in columnNameMapping) {
        continue;
      }
      const columnName = fieldName
        .toLowerCase()
        .replace(/[\s-]/g, "_")
        .replace(/[^0-9a-z_]/g, "")
        .replace(/^[^a-z]*/, "")
        .slice(0, 63);

      if (columnName === "") {
        throw new Error(
          `Aborting: Empty column name after parsing Airtable field names. Dataland column name must begin with a-z, only contain a-z, 0-9, and _, and have a maximum of 63 characters. ${JSON.stringify(
            {
              datalandColumnName: columnName,
              airtableFieldName: fieldName,
            }
          )}`
        );
      }
      if (columnName in fieldNameMapping) {
        throw new Error(
          `Aborting: Collision of parsed Airtable field names. Dataland column name must begin with a-z, only contain a-z, 0-9, and _, and have a maximum of 63 characters. ${JSON.stringify(
            {
              datalandColumnName: columnName,
              duplicateAirtableFieldNames: [
                fieldName,
                fieldNameMapping[columnName]!,
              ],
            }
          )}`
        );
      }
      columnNameMapping[fieldName] = columnName;
      fieldNameMapping[columnName] = fieldName;
    }
  }

  const rows: Record<string, Scalar>[] = [];
  for (const record of records) {
    const row: Record<string, Scalar> = {
      [RECORD_ID]: record.id,
    };
    for (const fieldName in record.fields) {
      const columnName = columnNameMapping[fieldName]!;
      const airtableValue = record.fields[fieldName]!;
      const parsedColumnValue = airtableValueToDatalandValue(airtableValue);
      row[columnName] = parsedColumnValue;
    }
    rows.push(row);
  }

  // NOTE(gab): Fields containing empty values (false, "", [], {}) are never
  // sent from Airtable. These fields need to be are added as null explicitly,
  // due to syncTables setting number columns to NaN if a value is missing
  for (const row of rows) {
    for (const columnName in fieldNameMapping) {
      if (columnName in row) {
        continue;
      }
      row[columnName] = null;
    }
  }
  return { rows, fieldNameMapping };
};

const cronHandler = async () => {
  const { rows, fieldNameMapping } = await readRowsFromAirtable();
  const table = tableFromJSON(rows);
  const batch = tableToIPC(table);

  const columnAnnotations: Record<
    string,
    { columnAnnotations: Record<string, string> }
  > = {};
  for (const columnName in fieldNameMapping) {
    const fieldName = fieldNameMapping[columnName]!;
    columnAnnotations[columnName] = {
      columnAnnotations: {
        [AIRTABLE_FIELD_NAME]: fieldName,
      },
    };
  }

  const tableSyncRequest: TableSyncRequest = {
    tableName: getDatalandTableName(),
    arrowRecordBatches: [batch],
    primaryKeyColumnNames: [RECORD_ID],
    transactionAnnotations: {},
    tableAnnotations: {},
    columnAnnotations,
    dropExtraColumns: true,
    deleteExtraRows: true,
  };
  const db = getDbClient();
  await db.tableSync(tableSyncRequest);
  console.log(`Import - Airtable import complete. Row count: ${rows.length}`);
};

console.log("register");
registerCronHandler(cronHandler);

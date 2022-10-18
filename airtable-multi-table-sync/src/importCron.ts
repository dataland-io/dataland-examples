import { tableFromJSON, tableToIPC } from "@apache-arrow/es2015-esm";
import {
  getEnv,
  registerCronHandler,
  TableSyncRequest,
  getDbClient,
  Scalar,
} from "@dataland-io/dataland-sdk";
import Airtable from "airtable";
import {
  AirtableRecordValue,
  AIRTABLE_FIELD_NAME,
  RECORD_ID,
  SyncTarget,
  AirtableRecord,
  getSyncTargets,
} from "./common";

const airtableValueToDatalandValue = (value: AirtableRecordValue): Scalar => {
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

const getAirtableRecords = async (
  syncTarget: SyncTarget
): Promise<readonly AirtableRecord[]> => {
  const airtableBase = new Airtable({
    apiKey: getEnv("AIRTABLE_API_KEY"),
  }).base(syncTarget.base_id);
  const airtableTable = airtableBase(syncTarget.table_id);

  const records: readonly AirtableRecord[] = await airtableTable
    .select({
      pageSize: 100,
      view: syncTarget.view_id,
    })
    .all();
  return records;
};

type ColumnName = string;
type FieldName = string;
type Rows = Record<string, Scalar>[];
type FieldNameMapping = Record<ColumnName, FieldName>;
const readRowsFromAirtable = async (
  syncTarget: SyncTarget
): Promise<[Rows, FieldNameMapping] | "error"> => {
  const records = await getAirtableRecords(syncTarget);

  const columnNameMapping: Record<FieldName, ColumnName> = {};
  const fieldNameMapping: Record<ColumnName, FieldName> = {};
  for (const record of records) {
    for (const fieldName in record.fields) {
      // NOTE(gab): Skip omitted fields.
      if (syncTarget.omit_fields?.has(fieldName)) {
        continue;
      }
      // NOTE(gab): Skip already added fields.
      if (fieldName in columnNameMapping) {
        continue;
      }
      const columnName = fieldName
        .toLowerCase()
        .replace(/^[^a-z]+/, "")
        .replace(/[^0-9a-z_]/g, "_")
        .slice(0, 63);

      if (columnName === "") {
        console.error(
          `Import - Skipping sync of dataland table name "${syncTarget.dataland_table_name}": Empty column name after parsing Airtable field names. Dataland column name must begin with a-z, only contain a-z, 0-9, and _, and have a maximum of 63 characters.`,
          {
            datalandColumnName: columnName,
            airtableFieldName: fieldName,
          }
        );
        return "error";
      }
      if (columnName in fieldNameMapping) {
        console.error(
          `Import - Skipping sync of dataland table name "${syncTarget.dataland_table_name}": Collision of parsed Airtable field names. Dataland column name must begin with a-z, only contain a-z, 0-9, and _, and have a maximum of 63 characters.`,
          {
            parsedDatalandColumnName: columnName,
            duplicateAirtableFieldNames: [
              fieldName,
              fieldNameMapping[columnName]!,
            ],
          }
        );
        return "error";
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
      const columnName = columnNameMapping[fieldName];
      // NOTE(gab): Skip omitted fields.
      if (columnName == null) {
        continue;
      }
      const airtableValue = record.fields[fieldName]!;
      const parsedColumnValue = airtableValueToDatalandValue(airtableValue);
      row[columnName] = parsedColumnValue;
    }
    rows.push(row);
  }

  // NOTE(gab): Fields containing empty values (false, "", [], {}) are never
  // sent from Airtable. These fields need to be are added as null explicitly,
  // due to syncTables setting number columns to NaN if a value is missing.
  for (const row of rows) {
    for (const columnName in fieldNameMapping) {
      if (columnName in row) {
        continue;
      }
      row[columnName] = null;
    }
  }
  return [rows, fieldNameMapping];
};

const cronHandler = async () => {
  const syncTargets = getSyncTargets();
  if (syncTargets === "error") {
    return;
  }

  for (const syncTarget of syncTargets) {
    const response = await readRowsFromAirtable(syncTarget);
    if (response === "error") {
      // NOTE(gab): Skip failing table and continue to next.
      continue;
    }
    const [rows, fieldNameMapping] = response;

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
      tableName: syncTarget.dataland_table_name,
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
    console.log(
      `Import - Successfully imported dataland table "${syncTarget.dataland_table_name}". Row count: ${rows.length}`
    );
  }
};
console.log("HEEYEYY");
registerCronHandler(cronHandler);

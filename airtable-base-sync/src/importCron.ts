import { tableFromJSON, tableToIPC } from "@apache-arrow/es2015-esm";
import {
  registerCronHandler,
  Scalar,
  SyncTable,
  syncTables,
} from "@dataland-io/dataland-sdk-worker";
import Airtable, { Attachment, Collaborator } from "airtable";
import {
  AIRTABLE_API_KEY,
  RECORD_ID,
  SYNC_TABLES_MARKER,
  AIRTABLE_SYNC_MAPPING_JSON,
} from "./constants";

const airtable_sync_mapping_json_parsed = JSON.parse(
  AIRTABLE_SYNC_MAPPING_JSON
);

type AirtableValue =
  | undefined
  | string
  | number
  | boolean
  | Collaborator
  | ReadonlyArray<Collaborator>
  | ReadonlyArray<string>
  | ReadonlyArray<Attachment>;

const parseAirtableValue = (value: AirtableValue): Scalar => {
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
  // updating values.
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

const readFromAirtable = async (
  base_id: string,
  table_id: string,
  view_id: string,
  read_fields_list: string
): Promise<Record<string, Scalar>[]> => {
  const records: Record<string, any>[] = [];

  let fields_array: (string | number)[] = [];
  if (read_fields_list == "ALL") {
    // if no fields_list is provided, get all fields
  } else {
    fields_array = read_fields_list.split(",").map((field) => field.trim());
  }

  const airtableBase = new Airtable({
    apiKey: AIRTABLE_API_KEY,
  }).base(base_id);

  const airtableTable = airtableBase(table_id);

  await new Promise((resolve, error) => {
    airtableTable
      .select({
        pageSize: 100,
        view: view_id,
        fields: fields_array,
      })
      .eachPage(
        (pageRecords, fetchNextPage) => {
          const fieldNames: Set<string> = new Set();
          for (const record of pageRecords) {
            for (const fieldName in record.fields) {
              fieldNames.add(fieldName);
            }
          }

          for (const record of pageRecords) {
            const parsedRecord: Record<string, Scalar> = {
              [RECORD_ID]: record.id,
            };
            for (const columnName in record.fields) {
              const columnValue = record.fields[columnName];
              const parsedColumnValue = parseAirtableValue(columnValue);
              parsedRecord[columnName] = parsedColumnValue;
            }

            // NOTE(gab): Fields containing empty values (false, "", [], {}) are
            // never sent from Airtable. These fields are added as null explicitly.
            // This is due to syncTables having an issue of setting number cells to
            // NaN if the column name is excluded from the row.
            for (const columnName of fieldNames) {
              if (columnName in parsedRecord) {
                continue;
              }
              parsedRecord[columnName] = null;
            }

            records.push(parsedRecord);
          }

          fetchNextPage();
        },
        (err) => {
          if (err) {
            console.error("Import fetch rows failed - ", err);
            error();
            return;
          }
          resolve(true);
        }
      );
  });
  return records;
};

const cronHandler = async () => {
  console.log("Airtable sync starting");

  // construct an iterable array of objects which has table id, view id, and fields list

  const sync_targets_array = airtable_sync_mapping_json_parsed.sync_targets;

  for (const sync_target of sync_targets_array) {
    const records = await readFromAirtable(
      sync_target.base_id,
      sync_target.table_id,
      sync_target.view_id,
      sync_target.read_fields_list.join(",")
    );

    const table = tableFromJSON(records);
    const batch = tableToIPC(table);

    const syncTable: SyncTable = {
      tableName: "Airtable - " + sync_target.table_name,
      arrowRecordBatches: [batch],
      identityColumnNames: [RECORD_ID],
    };

    await syncTables({
      syncTables: [syncTable],
      transactionAnnotations: {
        [SYNC_TABLES_MARKER]: "true",
      },
    });
  }
};

registerCronHandler(cronHandler);

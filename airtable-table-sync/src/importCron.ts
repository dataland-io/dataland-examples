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
  AIRTABLE_BASE_ID,
  AIRTABLE_TABLE_NAME,
  AIRTABLE_VIEW_NAME,
  DATALAND_TABLE_NAME,
  RECORD_ID,
  SYNC_TABLES_MARKER,
} from "./constants";

const airtableBase = new Airtable({
  apiKey: AIRTABLE_API_KEY,
}).base(AIRTABLE_BASE_ID);
const airtableTable = airtableBase(AIRTABLE_TABLE_NAME);

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

const readFromAirtable = async (): Promise<Record<string, Scalar>[]> => {
  const records: Record<string, any>[] = [];

  await new Promise((resolve, error) => {
    airtableTable
      .select({
        pageSize: 100,
        view: AIRTABLE_VIEW_NAME,
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
  const records = await readFromAirtable();

  const table = tableFromJSON(records);
  const batch = tableToIPC(table);

  const syncTable: SyncTable = {
    tableName: DATALAND_TABLE_NAME,
    arrowRecordBatches: [batch],
    identityColumnNames: [RECORD_ID],
  };

  await syncTables({
    syncTables: [syncTable],
    transactionAnnotations: {
      [SYNC_TABLES_MARKER]: "true",
    },
  });
};

registerCronHandler(cronHandler);

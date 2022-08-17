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
  // the reason these fields are parsed as strings is that when sending an update to airtable,
  // airtable expects lists to be in this format: "x,y,z" or '"x","y","z"', and not in its
  // stringified json form '["x", "y", "z"]'. this allows us to not need any additional parsing on
  // updating values
  //
  // field types that NEEDS to be parsed as a concatenated string:
  // - Linked record (list of strings)
  // - Multiple select (list of strings)
  //
  // additionally all lookup fields containing only strings or numbers are also parsed as a concatenated string,
  // since the field type cannot be known in-beforehand. this also makes the UI more consistent and has no impact
  // in any other way, since this field is precomputed
  //
  // full list of fields: https://datalandhq.quip.com/JeqZAWbt5Z9o/Module-planning-Airtable#temp:C:cZHf643296828573be85cea3bb62
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
  const allRecords: Record<string, any>[] = [];

  await new Promise((resolve, error) => {
    airtableTable
      .select({
        pageSize: 100,
        view: AIRTABLE_VIEW_NAME,
      })
      .eachPage(
        (records, fetchNextPage) => {
          const fieldNames: Set<string> = new Set();
          for (const record of records) {
            for (const fieldName in record.fields) {
              fieldNames.add(fieldName);
            }
          }

          for (const record of records) {
            const parsedRecord: Record<string, Scalar> = {
              [RECORD_ID]: record.id,
            };
            for (const columnName in record.fields) {
              const columnValue = record.fields[columnName];
              const parsedColumnValue = parseAirtableValue(columnValue);
              parsedRecord[columnName] = parsedColumnValue;
            }

            // NOTE(gab): fields containing empty values (false, "", [], {}) are
            // never sent from airtable. these fields are added as null explicitly.
            // this also means we have no way of getting a completely empty column from
            // airtable since they will never send a value with that field.
            // for (const columnName of fieldNames) {
            //   if (columnName in parsedRecord) {
            //     continue;
            //   }
            //   parsedRecord[columnName] = null;
            // }

            allRecords.push(parsedRecord);
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
  return allRecords;
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

  await syncTables({ syncTables: [syncTable] });
  console.log("Sync done");
};

registerCronHandler(cronHandler);

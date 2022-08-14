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

const parseAirableValue = (value: AirtableValue): Scalar => {
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

  if (typeof value === "object") {
    return JSON.stringify(value);
  }

  console.error("Airtable Sync - Could not parse value", { value });
  return null;
};

const readFromAirtable = async (): Promise<Record<string, any>[]> => {
  const allRecords: Record<string, any>[] = [];

  await new Promise((resolve, error) => {
    airtableTable
      .select({
        pageSize: 100,
        view: AIRTABLE_VIEW_NAME,
      })
      .eachPage(
        (records, fetchNextPage) => {
          const columnNames: Set<string> = new Set();
          for (const record of records) {
            for (const columnName in record.fields) {
              columnNames.add(columnName);
            }
          }

          for (const record of records) {
            const parsedRecord: Record<string, Scalar> = {
              [RECORD_ID]: record.id,
            };
            for (const columnName in record.fields) {
              const columnValue = record.fields[columnName];
              const parsedColumnValue = parseAirableValue(columnValue);
              parsedRecord[columnName] = parsedColumnValue;
            }

            // NOTE(gab): fields containing empty values (false, "", [], {}) are
            // never sent from airtable. these fields are added as null explicitly.
            // this also means a completely empty column will now show up at all,
            // since no value will be returned from the api from that field
            for (const columnName of columnNames) {
              if (
                columnName in parsedRecord ||
                columnName === "_dataland_key" ||
                columnName === "_dataland_ordinal"
              ) {
                continue;
              }
              parsedRecord[columnName] = null;
            }

            allRecords.push(parsedRecord);
          }

          fetchNextPage();
        },
        (err) => {
          if (err) {
            console.error("Airtable Fetch Error -", err);
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

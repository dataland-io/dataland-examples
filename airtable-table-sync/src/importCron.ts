import { tableFromJSON, tableToIPC } from "@apache-arrow/es2015-esm";
import {
  getEnv,
  registerCronHandler,
  Scalar,
  TableSyncRequest,
  getDbClient,
} from "@dataland-io/dataland-sdk";
import Airtable, { Attachment, Collaborator } from "airtable";
import { RECORD_ID } from "./constants";

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

type ColumnName = string;
type FieldName = string;
const readFromAirtable = async (): Promise<{
  records: Record<string, Scalar>[];
  fieldNameMapping: Record<ColumnName, FieldName>;
}> => {
  const AIRTABLE_FIELDS_LIST = getEnv("AIRTABLE_FIELDS_LIST");

  let fields: string[] = [];
  if (AIRTABLE_FIELDS_LIST !== "ALL") {
    fields = AIRTABLE_FIELDS_LIST.split(",").map((field) => field.trim());
  }

  const airtableBase = new Airtable({
    apiKey: getEnv("AIRTABLE_API_KEY"),
  }).base(getEnv("AIRTABLE_BASE_ID"));
  const airtableTable = airtableBase(getEnv("AIRTABLE_TABLE_NAME"));

  const columnNameMapping: Record<FieldName, ColumnName> = {};
  const fieldNameMapping: Record<ColumnName, FieldName> = {};
  const records: Record<string, any>[] = [];
  await new Promise((resolve, error) => {
    airtableTable
      .select({
        pageSize: 100,
        view: getEnv("AIRTABLE_VIEW_NAME"),
        fields,
      })
      .eachPage(
        (pageRecords, fetchNextPage) => {
          for (const record of pageRecords) {
            for (const fieldName in record.fields) {
              if (fieldName in columnNameMapping) {
                continue;
              }
              const columnName = fieldName
                .toLowerCase()
                .replace(/[\s-]/g, "_")
                .replace(/[^0-9a-z_]/g, "")
                .slice(0, 20);
              if (columnName in fieldNameMapping) {
                console.error(
                  "Dataland column name collision - contact the Dataland Team",
                  {
                    datalandColumnName: columnName,
                    duplicateAirtableFieldNames: [
                      fieldName,
                      fieldNameMapping[columnName],
                    ],
                  }
                );
              }
              columnNameMapping[fieldName] = columnName;
              fieldNameMapping[columnName] = fieldName;
            }
          }

          for (const record of pageRecords) {
            const parsedRecord: Record<string, Scalar> = {
              [RECORD_ID]: record.id,
            };
            for (const fieldName in record.fields) {
              const columnName = columnNameMapping[fieldName]!;
              const columnValue = record.fields[columnName];
              const parsedColumnValue = parseAirtableValue(columnValue);
              parsedRecord[columnName] = parsedColumnValue;
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

  // NOTE(gab): Fields containing empty values (false, "", [], {}) are
  // never sent from Airtable. These fields are added as null explicitly.
  // This is due to syncTables having an issue of setting number cells to
  // NaN if the column name is excluded from the row.
  for (const record of records) {
    for (const columnName of Object.values(columnNameMapping)) {
      if (columnName in record) {
        continue;
      }
      record[columnName] = null;
    }
  }
  return { records, fieldNameMapping };
};

export const validateSqlIdentifier = (sqlIdentifier: string): boolean => {
  return /^[a-z][_a-z0-9]{0,62}$/.test(sqlIdentifier);
};

const cronHandler = async () => {
  const { records, fieldNameMapping } = await readFromAirtable();

  const table = tableFromJSON(records);
  const batch = tableToIPC(table);

  const DATALAND_TABLE_NAME = getEnv("DATALAND_TABLE_NAME");
  if (!validateSqlIdentifier(DATALAND_TABLE_NAME)) {
    console.error(
      `Aborting: Invalid table name: "${DATALAND_TABLE_NAME}". Must begin with a-z, only contain a-z, 0-9, and _, and have a maximum of 63 characters.`
    );
    return;
  }

  const columnAnnotations: Record<
    string,
    { columnAnnotations: Record<string, string> }
  > = {};
  for (const columnName in fieldNameMapping) {
    const fieldName = fieldNameMapping[columnName]!;
    columnAnnotations[columnName] = {
      columnAnnotations: {
        "dataland.io/airtable-field-name": fieldName,
      },
    };
  }

  const tableSyncRequest: TableSyncRequest = {
    tableName: DATALAND_TABLE_NAME,
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
};

registerCronHandler(cronHandler);

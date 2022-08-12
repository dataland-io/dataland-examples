import { tableFromJSON, tableToIPC } from "@apache-arrow/es2015-esm";
import {
  getEnv,
  syncTables,
  SyncTable,
  Transaction,
  registerTransactionHandler,
  registerCronHandler,
  getCatalogSnapshot,
  querySqlSnapshot,
  unpackRows,
  Schema,
  Mutation,
  runMutations,
  Scalar,
  getCatalogMirror,
} from "@dataland-io/dataland-sdk-worker";
import Airtable, {
  Attachment,
  Collaborator,
  FieldSet as AirtableFieldSet,
  RecordData as AirtableRecordData,
  Table as AirtableTable,
} from "airtable";

const RECORD_ID = "record-id";

const VIEW_NAME = getEnv("AIRTABLE_VIEW_NAME");
const DATALAND_TABLE_NAME = getEnv("DATALAND_TABLE_NAME");
const airtableBase = new Airtable({
  apiKey: getEnv("AIRTABLE_API_KEY"),
}).base(getEnv("AIRTABLE_BASE_ID"));
const airtableTable = airtableBase(getEnv("AIRTABLE_TABLE_NAME"));

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

  // if (Array.isArray(value)) {
  //   if (value.length === 0) {
  //     return null;
  //   }
  //   if (typeof value[0] !== "object") {
  //     return value.join(", ");
  //   }
  //   return JSON.stringify(value);
  // }

  if (typeof value === "object") {
    // if (isEmpty(value)) {
    //   return null;
    // }
    return JSON.stringify(value);
  }

  console.error("Airtable Sync - Could not parse value", { value });
  return null;
};

const readFromAirtable = async (
  columnNames: string[]
): Promise<Record<string, any>[]> => {
  const allRecords: Record<string, any>[] = [];

  await new Promise((resolve, error) => {
    airtableTable
      .select({
        pageSize: 100,
        // maxRecords: 3,
        view: VIEW_NAME,
      })
      .eachPage(
        (records, fetchNextPage) => {
          // console.log(
          //   "records before",
          //   JSON.stringify(records[0]!.fields, null, 2)
          // );

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
            // never sent from airtable. these fields are added as null explicitly
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
  // console.log("records after", JSON.stringify(allRecords[0]!, null, 2)!);
  return allRecords;
};

const cronHandler = async () => {
  console.log("Start sync");

  const { tableDescriptors } = await getCatalogMirror();
  const tableDescriptor = tableDescriptors.find(
    (t) => t.tableName === DATALAND_TABLE_NAME
  );

  if (tableDescriptor == null) {
    console.error("Airtable Sync - Could not find table descriptor");
    return;
  }

  const columnNames = tableDescriptor.columnDescriptors.map(
    (c) => c.columnName
  );

  const records = await readFromAirtable(columnNames);

  const table = tableFromJSON(records);
  const batch = tableToIPC(table);

  const syncTable: SyncTable = {
    tableName: DATALAND_TABLE_NAME,
    arrowRecordBatches: [batch],
    identityColumnNames: [RECORD_ID],
    keepExtraColumns: true,
  };

  await syncTables({ syncTables: [syncTable] });
  console.log("Sync done");
};

const airtableUpdateRows = async (
  table: AirtableTable<AirtableFieldSet>,
  updateRows: AirtableRecordData<Partial<AirtableFieldSet>>[]
) => {
  console.log("UPDATE", updateRows);
  return await new Promise((resolve, error) => {
    table.update(updateRows, { typecast: true }, (err) => {
      if (err) {
        console.error("Airtable Update Rows - Failed to update rows", {
          error: err,
        });
        error(err);
        return;
      }
      resolve(true);
    });
  });
};

const airtableCreateRows = async (
  table: AirtableTable<AirtableFieldSet>,
  createRows: { fields: AirtableFieldSet }[]
): Promise<string[]> => {
  return await new Promise((resolve, error) => {
    table.create(createRows, { typecast: true }, (err, records) => {
      if (err || records == null) {
        console.error("Airtable Create Rows - Failed to update rows", {
          error: err,
        });
        error(err);
        return;
      }

      const recordIds = records.map((record) => record.getId());
      resolve(recordIds);
    });
  });
};

const airtableDestoryRows = async (
  table: AirtableTable<AirtableFieldSet>,
  recordIds: string[]
) => {
  return await new Promise((resolve, error) => {
    table.destroy(recordIds, (err) => {
      if (err) {
        console.error("Airtable Create Rows - Failed to update rows", {
          error: err,
        });
        error(err);
        return;
      }
      resolve(true);
    });
  });
};

const airtablifyValue = (value: any) => {
  return value ?? null;
  // try {
  //   const parsed = JSON.parse(value);
  //   if (typeof parsed === "object") {
  //     return parsed;
  //   }
  // } catch (e) {
  //   // do nothing
  // }
  // return value ?? null;dfs
};

const handler2 = async (transaction: Transaction) => {
  const { tableDescriptors } = await getCatalogSnapshot({
    logicalTimestamp: transaction.logicalTimestamp,
  });

  const response = await querySqlSnapshot({
    logicalTimestamp: transaction.logicalTimestamp - 1,
    sqlQuery: `select "_dataland_key", "${RECORD_ID}" from "${DATALAND_TABLE_NAME}"`,
  });

  const rows = unpackRows(response);

  const recordIdMap: Record<number, string> = {};
  for (const row of rows) {
    const key = row["_dataland_key"] as number;
    const recordId = row[RECORD_ID] as string;
    recordIdMap[key] = recordId;
  }

  const tableDescriptor = tableDescriptors.find(
    (descriptor) => descriptor.tableName === DATALAND_TABLE_NAME
  );
  if (tableDescriptor == null) {
    console.error(
      "Airtable Sync - Could not find table descriptor by table name",
      { tableName: DATALAND_TABLE_NAME }
    );
    return;
  }

  const schema = new Schema(tableDescriptors);

  const getColumnName = (columnUuid: string): string | null => {
    const column = tableDescriptor.columnDescriptors.find(
      (c) => c.columnUuid === columnUuid
    );
    if (column == null) {
      return null;
    }
    return column.columnName;
  };

  for (const mutation of transaction.mutations) {
    if (
      mutation.kind !== "insert_rows" &&
      mutation.kind !== "update_rows" &&
      mutation.kind !== "delete_rows"
    ) {
      return;
    }

    if (tableDescriptor.tableUuid !== mutation.value.tableUuid) {
      continue;
    }

    switch (mutation.kind) {
      case "insert_rows": {
        const createRows: { fields: AirtableFieldSet }[] = [];
        for (let i = 0; i < mutation.value.rows.length; i++) {
          const createRow: AirtableFieldSet = {};
          const { key, values } = mutation.value.rows[i]!;

          for (let j = 0; j < mutation.value.rows.length; j++) {
            const rowValue = values[j];
            const columnUuid = mutation.value.columnMapping[i];
            const columnName = getColumnName(columnUuid);
            if (columnName == null) {
              console.error(
                "Airtable Sync - Could not find column by column uuid",
                { columnUuid }
              );
              return;
            }
            if (columnName === "_dataland_ordinal") {
              continue;
            }
            createRow[columnName] = airtablifyValue(rowValue?.value);
          }

          createRows.push({ fields: createRow });
        }

        const recordIds = await airtableCreateRows(airtableTable, createRows);
        const mutations: Mutation[] = [];
        for (let i = 0; i < recordIds.length; i++) {
          const recordId = recordIds[i];
          const datalandKey = mutation.value.rows[i]?.key;

          if (datalandKey == null) {
            console.error(
              "Airtable Sync - Could not find dataland key for record id",
              { recordId }
            );
            return;
          }

          const update = schema.makeUpdateRows(
            DATALAND_TABLE_NAME,
            datalandKey,
            {
              [RECORD_ID]: recordId,
            }
          );
          mutations.push(update);
        }
        await runMutations({ mutations });

        break;
      }
      case "update_rows": {
        const updateRows: AirtableRecordData<Partial<AirtableFieldSet>>[] = [];
        for (let i = 0; i < mutation.value.rows.length; i++) {
          const updateRow: Partial<AirtableFieldSet> = {};
          const { key, values } = mutation.value.rows[i]!;

          for (let j = 0; j < mutation.value.rows.length; j++) {
            const rowValue = values[j];
            const columnUuid = mutation.value.columnMapping[i];
            const columnName = getColumnName(columnUuid);
            if (columnName == null) {
              console.error(
                "Airtable Sync - Could not find column by column uuid",
                { columnUuid }
              );
              return;
            }
            if (columnName === "_dataland_ordinal") {
              continue;
            }
            updateRow[columnName] = airtablifyValue(rowValue?.value);
          }

          updateRows.push({ id: recordIdMap[key], fields: updateRow });
        }
        await airtableUpdateRows(airtableTable, updateRows);
        break;
      }
      case "delete_rows": {
        const deleteRows: string[] = [];
        for (let i = 0; i < mutation.value.keys.length; i++) {
          const key = mutation.value.keys[i];
          const recordId = recordIdMap[key];

          if (recordId == null) {
            console.error("Airtable Sync - Could not find Airtable record id", {
              datalandKey: key,
            });
            return;
          }

          deleteRows.push(recordId);
        }

        if (deleteRows.length === 0) {
          continue;
        }
        await airtableDestoryRows(airtableTable, deleteRows);
        break;
      }
    }
  }
};

console.log("REFISTERING AIRTABLE HANDELR");
registerTransactionHandler(handler2);
registerCronHandler(cronHandler);

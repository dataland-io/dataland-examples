import {
  Transaction,
  getCatalogSnapshot,
  querySqlSnapshot,
  unpackRows,
  Schema,
  Mutation,
  runMutations,
  registerTransactionHandler,
  Uuid,
} from "@dataland-io/dataland-sdk-worker";
import Airtable, {
  FieldSet as AirtableFieldSet,
  RecordData as AirtableRecordData,
  Table as AirtableTable,
} from "airtable";
import {
  AIRTABLE_API_KEY,
  AIRTABLE_BASE_ID,
  AIRTABLE_TABLE_NAME,
  ALLOW_WRITEBACK_BOOLEAN,
  DATALAND_TABLE_NAME,
  RECORD_ID,
} from "./constants";

const airtableBase = new Airtable({
  apiKey: AIRTABLE_API_KEY,
}).base(AIRTABLE_BASE_ID);
const airtableTable = airtableBase(AIRTABLE_TABLE_NAME);

const AIRTABLE_MAX_UPDATES = 10;
const chunkAirtablePayload = <T>(array: T[]) => {
  const chunks: T[][] = [];

  for (let i = 0; i < array.length; i += AIRTABLE_MAX_UPDATES) {
    const chunk = array.slice(i, i + AIRTABLE_MAX_UPDATES);
    chunks.push(chunk);
  }
  return chunks;
};

const airtableUpdateRows = async (
  table: AirtableTable<AirtableFieldSet>,
  updateRows: AirtableRecordData<Partial<AirtableFieldSet>>[]
) => {
  const chunks = chunkAirtablePayload(updateRows);
  for (const chunk of chunks) {
    await new Promise((resolve) => {
      table.update(chunk, { typecast: true }, (err) => {
        if (err != null) {
          console.error("Writeback - Failed to update rows", {
            error: err,
            updateRows,
          });
          return;
        }
        resolve(true);
      });
    });
  }
};

const airtableCreateRows = async (
  table: AirtableTable<AirtableFieldSet>,
  createRows: { fields: AirtableFieldSet }[]
): Promise<string[]> => {
  const recordIds: string[] = [];
  const chunks = chunkAirtablePayload(createRows);
  for (const chunk of chunks) {
    const chunkRecordIds = await new Promise<string[]>((resolve) => {
      table.create(chunk, { typecast: true }, (err, records) => {
        if (err != null || records == null) {
          console.error("Writeback - Failed to create rows", {
            error: err,
            createRows,
          });
          return;
        }

        const recordIds = records.map((record) => record.getId());
        resolve(recordIds);
      });
    });
    recordIds.push(...chunkRecordIds);
  }
  return recordIds;
};

const airtableDestroyRows = async (
  table: AirtableTable<AirtableFieldSet>,
  recordIds: string[]
) => {
  const chunks = chunkAirtablePayload(recordIds);
  for (const chunk of chunks) {
    await new Promise((resolve) => {
      table.destroy(chunk, (err) => {
        if (err != null) {
          console.error("Writeback - Failed to update rows", {
            error: err,
            recordIds,
          });
          return;
        }
        resolve(true);
      });
    });
  }
};

const transactionHandler = async (transaction: Transaction) => {
  const { tableDescriptors } = await getCatalogSnapshot({
    logicalTimestamp: transaction.logicalTimestamp - 1,
  });

  const response = await querySqlSnapshot({
    logicalTimestamp: transaction.logicalTimestamp - 1,
    sqlQuery: `select "_dataland_key", "${RECORD_ID}" from "${DATALAND_TABLE_NAME}"`,
  });

  const rows = unpackRows(response);
  const tableDescriptor = tableDescriptors.find(
    (descriptor) => descriptor.tableName === DATALAND_TABLE_NAME
  );
  if (tableDescriptor == null) {
    console.error("Writeback - Could not find table descriptor by table name", {
      tableName: DATALAND_TABLE_NAME,
    });
    return;
  }

  const schema = new Schema(tableDescriptors);

  // NOTE(gab): be careful here - we fetch the rows BEFORE the timestamp,
  // meaning an inserted row will not appear in this map
  const recordIdMap: Record<number, string> = {};
  for (const row of rows) {
    const key = row["_dataland_key"] as number;
    const recordId = row[RECORD_ID] as string;
    recordIdMap[key] = recordId;
  }

  const columnNameMap: Record<Uuid, string> = {};
  for (const columnDescriptor of tableDescriptor.columnDescriptors) {
    columnNameMap[columnDescriptor.columnUuid] = columnDescriptor.columnName;
  }

  for (const mutation of transaction.mutations) {
    if (
      mutation.kind !== "insert_rows" &&
      mutation.kind !== "update_rows" &&
      mutation.kind !== "delete_rows"
    ) {
      continue;
    }

    if (tableDescriptor.tableUuid !== mutation.value.tableUuid) {
      continue;
    }

    switch (mutation.kind) {
      case "insert_rows": {
        const createRows: { fields: AirtableFieldSet }[] = [];
        for (let i = 0; i < mutation.value.rows.length; i++) {
          const createRow: AirtableFieldSet = {};
          const { values } = mutation.value.rows[i]!;

          for (let j = 0; j < mutation.value.rows.length; j++) {
            const rowValue = values[j]!;
            const columnUuid = mutation.value.columnMapping[i]!;
            const columnName = columnNameMap[columnUuid];
            if (columnName == null) {
              console.error(
                "Writeback - Could not find column name by column uuid",
                { columnUuid }
              );
              continue;
            }
            if (columnName === "_dataland_ordinal") {
              continue;
            }

            createRow[columnName] = rowValue?.value;
          }

          createRows.push({ fields: createRow });
        }

        // TODO(gab): if deciding to sort rows as in the airtable view, make sure
        // records are returned in the same order that they are passed if multiple
        // create rows are in the same transaction
        if (createRows.length === 0) {
          continue;
        }
        const recordIds = await airtableCreateRows(airtableTable, createRows);
        if (recordIds.length !== mutation.value.rows.length) {
          console.error(
            "Writeback - Created rows in Dataland and created rows in Airtable was of different length. State will be reconciled in next Airtable Sync",
            {
              datalandRowsLength: mutation.value.rows.length,
              airtableRecordsLength: recordIds.length,
            }
          );
          continue;
        }

        const mutations: Mutation[] = [];
        for (let i = 0; i < recordIds.length; i++) {
          const recordId = recordIds[i]!;
          const datalandKey = mutation.value.rows[i]!.key;

          recordIdMap[datalandKey] = recordId;

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

          for (let j = 0; j < values.length; j++) {
            const rowValue = values[j]!;
            const columnUuid = mutation.value.columnMapping[i]!;
            const columnName = columnNameMap[columnUuid];
            console.log(columnNameMap);
            if (columnName == null) {
              console.error(
                "Writeback - Could not find column name by column uuid",
                { columnUuid }
              );
              continue;
            }

            if (columnName === "_dataland_ordinal") {
              continue;
            }

            // @ts-ignore - NOTE(gab): nulls are used to clear a value from airtable,
            // but is not in their type system for unknown reason.
            updateRow[columnName] = rowValue?.value ?? null;
          }

          const recordId = recordIdMap[key];
          if (recordId == null) {
            console.error(
              "Writeback - Could not find record-id by dataland key",
              {
                key,
              }
            );
            continue;
          }
          updateRows.push({ id: recordId, fields: updateRow });
        }

        if (updateRows.length === 0) {
          continue;
        }
        await airtableUpdateRows(airtableTable, updateRows);
        break;
      }
      case "delete_rows": {
        const deleteRows: string[] = [];
        for (let i = 0; i < mutation.value.keys.length; i++) {
          const key = mutation.value.keys[i]!;
          const recordId = recordIdMap[key];
          if (recordId == null) {
            console.error(
              "Writeback - Could not find record-id by dataland key",
              {
                key,
              }
            );
            return;
          }

          deleteRows.push(recordId);
        }

        if (deleteRows.length === 0) {
          continue;
        }
        await airtableDestroyRows(airtableTable, deleteRows);
        break;
      }
    }
  }
};
//
if (ALLOW_WRITEBACK_BOOLEAN !== "true" && ALLOW_WRITEBACK_BOOLEAN !== "false") {
  console.error(
    `Writeback - 'ALLOW_WRITEBACK_BOOLEAN' invalid value '${ALLOW_WRITEBACK_BOOLEAN}', expected 'true' or 'false'.`
  );
}

if (ALLOW_WRITEBACK_BOOLEAN === "true") {
  registerTransactionHandler(transactionHandler);
}

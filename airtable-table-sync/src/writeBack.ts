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
  SYNC_TABLES_MARKER,
} from "./constants";

const airtableBase = new Airtable({
  apiKey: AIRTABLE_API_KEY,
}).base(AIRTABLE_BASE_ID);
const airtableTable = airtableBase(AIRTABLE_TABLE_NAME);

const AIRTABLE_MAX_UPDATES = 10;
const chunkAirtablePayload = <T>(payload: T[]) => {
  const chunks: T[][] = [];

  for (let i = 0; i < payload.length; i += AIRTABLE_MAX_UPDATES) {
    const chunk = payload.slice(i, i + AIRTABLE_MAX_UPDATES);
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
          console.error("Writeback - Failed to update rows in Airtable table", {
            error: err,
            updateRows,
          });
          resolve(false);
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
          console.error(
            "Writeback - Failed to insert rows into Airtable table",
            {
              error: err,
              createRows,
            }
          );
          resolve([]);
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
          console.error("Writeback - Failed to delete rows in Airtable table", {
            error: err,
            recordIds,
          });
          resolve(false);
          return;
        }
        resolve(true);
      });
    });
  }
};

const insertRowsWriteback = async (
  mutation: Extract<Mutation, { kind: "insert_rows" }>,
  schema: Schema,
  columnNameMap: Record<Uuid, string>,
  recordIdMap: Record<number, string>
) => {
  const createRows: { fields: AirtableFieldSet }[] = [];
  const { rows, columnMapping } = mutation.value;

  for (let i = 0; i < rows.length; i++) {
    const createRow: AirtableFieldSet = {};
    const { values } = rows[i]!;

    for (let j = 0; j < values.length; j++) {
      const taggedScalar = values[j];
      const columnUuid = columnMapping[j]!;
      const columnName = columnNameMap[columnUuid];
      if (columnName == null) {
        console.error("Writeback - Could not find column name by column uuid", {
          columnUuid,
        });
        continue;
      }
      if (columnName === "_dataland_ordinal") {
        continue;
      }

      createRow[columnName] = taggedScalar?.value;
    }

    createRows.push({ fields: createRow });
  }

  if (createRows.length === 0) {
    return;
  }
  const recordIds = await airtableCreateRows(airtableTable, createRows);
  if (recordIds.length !== rows.length) {
    console.error(
      "Writeback - Created rows in Dataland and created rows in Airtable was of different length. State will be reconciled in next Airtable Sync",
      {
        datalandRowsLength: mutation.value.rows.length,
        airtableRecordsLength: recordIds.length,
      }
    );
    return;
  }

  const mutations: Mutation[] = [];
  for (let i = 0; i < recordIds.length; i++) {
    // NOTE(gab): recordIds are returned in the same order as the create records are sent,
    // therefore we can safely index them here
    const recordId = recordIds[i]!;
    const datalandKey = mutation.value.rows[i]!.key;

    recordIdMap[datalandKey] = recordId;

    const update = schema.makeUpdateRows(DATALAND_TABLE_NAME, datalandKey, {
      [RECORD_ID]: recordId,
    });
    mutations.push(update);
  }
  await runMutations({ mutations });
};

const updateRowsWriteback = async (
  mutation: Extract<Mutation, { kind: "update_rows" }>,
  columnNameMap: Record<Uuid, string>,
  recordIdMap: Record<number, string>
) => {
  const updateRows: AirtableRecordData<Partial<AirtableFieldSet>>[] = [];
  const { rows, columnMapping } = mutation.value;
  for (let i = 0; i < rows.length; i++) {
    const updateRow: Partial<AirtableFieldSet> = {};
    const { key, values } = rows[i]!;

    for (let j = 0; j < values.length; j++) {
      const taggedScalar = values[j];
      const columnUuid = columnMapping[j]!;
      const columnName = columnNameMap[columnUuid];
      if (columnName == null) {
        console.error("Writeback - Could not find column name by column uuid", {
          columnUuid,
        });
        continue;
      }

      if (columnName === "_dataland_ordinal") {
        continue;
      }

      // @ts-ignore - NOTE(gab): nulls are used to clear ANY field value from airtable.
      // the reason it is not in their type system is probably that they actually "expect"
      // the empty type for that field, "false", "", [] etc and not null. but since
      // they won't provide a schema, null is a nice way of clearing any cell
      updateRow[columnName] = taggedScalar?.value ?? null;
    }

    const recordId = recordIdMap[key];
    if (recordId == null) {
      console.error("Writeback - Could not find record id by dataland key", {
        key,
      });
      continue;
    }
    updateRows.push({ id: recordId, fields: updateRow });
  }

  if (updateRows.length === 0) {
    return;
  }
  await airtableUpdateRows(airtableTable, updateRows);
};

const deleteRowsWriteback = async (
  mutation: Extract<Mutation, { kind: "delete_rows" }>,
  recordIdMap: Record<number, string>
) => {
  const deleteRows: string[] = [];
  for (let i = 0; i < mutation.value.keys.length; i++) {
    const key = mutation.value.keys[i]!;
    const recordId = recordIdMap[key];
    if (recordId == null) {
      console.error("Writeback - Could not find record id by dataland key", {
        key,
      });
      continue;
    }

    deleteRows.push(recordId);
  }

  if (deleteRows.length === 0) {
    return;
  }
  await airtableDestroyRows(airtableTable, deleteRows);
};

const transactionHandler = async (transaction: Transaction) => {
  // NOTE(gab): updating a cell in dataland while airtable cron sync is running would
  // cause the synced data from airtable to be outdated. when the syncTables transaction
  // finally goes through, it would set the cell to its previous value which we expect.
  // the discrepancy would then be reconciled in the next sync. but if the transaction handler
  // is run on syncTables, the outdated cell change would propagate to airtable again,
  // permanently reverting the cell update.
  if (SYNC_TABLES_MARKER in transaction.transactionAnnotations) {
    transaction.mutations.forEach(console.log);
    return;
  }

  const response = await querySqlSnapshot({
    logicalTimestamp: transaction.logicalTimestamp - 1,
    sqlQuery: `select "_dataland_key", "${RECORD_ID}" from "${DATALAND_TABLE_NAME}"`,
  });
  const rows = unpackRows(response);

  const { tableDescriptors } = await getCatalogSnapshot({
    logicalTimestamp: transaction.logicalTimestamp - 1,
  });
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
        await insertRowsWriteback(mutation, schema, columnNameMap, recordIdMap);
        break;
      }
      case "update_rows": {
        await updateRowsWriteback(mutation, columnNameMap, recordIdMap);
        break;
      }
      case "delete_rows": {
        await deleteRowsWriteback(mutation, recordIdMap);
        break;
      }
    }
  }
};

if (ALLOW_WRITEBACK_BOOLEAN !== "true" && ALLOW_WRITEBACK_BOOLEAN !== "false") {
  console.error(
    `Writeback - 'ALLOW_WRITEBACK_BOOLEAN' invalid value '${ALLOW_WRITEBACK_BOOLEAN}', expected 'true' or 'false'.`
  );
}

if (ALLOW_WRITEBACK_BOOLEAN === "true") {
  registerTransactionHandler(transactionHandler, {
    filterTransactions: "handle-all",
  });
}

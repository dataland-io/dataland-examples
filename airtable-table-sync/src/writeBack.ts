import {
  Transaction,
  unpackRows,
  Mutation,
  registerTransactionHandler,
  getEnv,
  MutationsBuilder,
  getDbClient,
  valueToScalar,
  getHistoryClient,
} from "@dataland-io/dataland-sdk";
import Airtable, {
  FieldSet as AirtableFieldSet,
  RecordData as AirtableRecordData,
  Table as AirtableTable,
  Table,
} from "airtable";
import { RECORD_ID } from "./constants";

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
  airtableTable: Table<AirtableFieldSet>,
  mutation: Extract<Mutation["kind"], { oneofKind: "insertRows" }>,
  fieldNameMap: Record<string, string>,
  recordIdMap: Record<number, string>
) => {
  const createRows: { fields: AirtableFieldSet }[] = [];
  const { rows, columnNames } = mutation.insertRows;

  for (let i = 0; i < rows.length; i++) {
    const createRow: AirtableFieldSet = {};
    const { values: listValues } = rows[i]!;
    if (listValues == null) {
      continue;
    }
    const values = listValues.values;
    for (let j = 0; j < values.length; j++) {
      const scalar = valueToScalar(values[j]!);
      const columnName = columnNames[j]!;
      const fieldName = fieldNameMap[columnName];
      if (fieldName == null) {
        console.error("Writeback - Could not find field name by column name", {
          columnName,
        });
        continue;
      }

      createRow[fieldName] = scalar ?? undefined; // NOTE(gab): accepts undefined, not null
    }

    createRows.push({ fields: createRow });
  }

  if (createRows.length === 0) {
    return;
  }
  const recordIds = await airtableCreateRows(airtableTable, createRows);
  if (recordIds.length !== rows.length) {
    console.error(
      "Writeback - Created rows of different lengths. State will be reconciled in next Airtable Sync",
      {
        datalandRowsLength: mutation.insertRows.columnNames.length,
        airtableRecordsLength: recordIds.length,
      }
    );
    return;
  }

  const mutations = new MutationsBuilder();
  for (let i = 0; i < recordIds.length; i++) {
    // NOTE(gab): Record ids are returned from Airtable in the same order
    // as the records were sent, therefore we can safely assume the first index
    // of recordIds corresponds to the first index of the rows.
    const recordId = recordIds[i]!;
    const rowKey = rows[i]!.rowId;
    recordIdMap[rowKey] = recordId;

    mutations.updateRow(getEnv("DATALAND_TABLE_NAME"), rowKey, {
      [RECORD_ID]: recordId,
    });
    // mutations.push(update);
  }
  await mutations.run(getDbClient());
};

const updateRowsWriteback = async (
  airtableTable: Table<AirtableFieldSet>,
  mutation: Extract<Mutation["kind"], { oneofKind: "updateRows" }>,
  fieldNameMap: Record<string, string>,
  recordIdMap: Record<number, string>
) => {
  const updateRows: AirtableRecordData<Partial<AirtableFieldSet>>[] = [];
  const { rows, columnNames } = mutation.updateRows;
  for (let i = 0; i < rows.length; i++) {
    const updateRow: Partial<AirtableFieldSet> = {};
    const { rowId, values: listValues } = rows[i]!;
    if (listValues == null) {
      continue;
    }

    const values = listValues.values;
    for (let j = 0; j < values.length; j++) {
      const scalar = valueToScalar(values[j]!);
      const columnName = columnNames[j]!;
      const fieldName = fieldNameMap[columnName];
      if (fieldName == null) {
        console.error("Writeback - Could not find column name by column uuid", {
          columnName,
        });
        continue;
      }

      // @ts-ignore - NOTE(gab): Nulls are used to clear ANY field value from Airtable.
      // The reason it's not in their type system is probably that they"expect"
      // the empty type for that field: "false", "", [] etc and not null. But since
      // no schema is provided from their side, the correct "empty type" cannot be known,
      // and null is used.
      updateRow[fieldName] = scalar ?? null;
    }

    const recordId = recordIdMap[rowId];
    if (recordId == null) {
      console.error("Writeback - Could not find record id by dataland rowId", {
        rowId,
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
  airtableTable: Table<AirtableFieldSet>,
  mutation: Extract<Mutation["kind"], { oneofKind: "deleteRows" }>,
  recordIdMap: Record<number, string>
) => {
  const deleteRows: string[] = [];
  const rowIds = mutation.deleteRows.rowIds;
  for (const rowId of rowIds) {
    const recordId = recordIdMap[rowId];
    if (recordId == null) {
      console.error("Writeback - Could not find record id by dataland key", {
        rowId,
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
  const ALLOW_WRITEBACK_BOOLEAN = getEnv("ALLOW_WRITEBACK_BOOLEAN");
  if (
    ALLOW_WRITEBACK_BOOLEAN !== "true" &&
    ALLOW_WRITEBACK_BOOLEAN !== "false"
  ) {
    console.error(
      `Writeback - ABORTING: 'ALLOW_WRITEBACK_BOOLEAN' invalid value '${ALLOW_WRITEBACK_BOOLEAN}', expected 'true' or 'false'.`
    );
    return;
  }

  if (ALLOW_WRITEBACK_BOOLEAN !== "true") {
    return;
  }

  const airtableBase = new Airtable({
    apiKey: getEnv("AIRTABLE_API_KEY"),
  }).base(getEnv("AIRTABLE_BASE_ID"));
  const airtableTable = airtableBase(getEnv("AIRTABLE_TABLE_NAME"));

  const DATALAND_TABLE_NAME = getEnv("DATALAND_TABLE_NAME");

  const history = getHistoryClient();
  const response = await history.querySqlSnapshot({
    logicalTimestamp: transaction.logicalTimestamp - 1,
    sqlQuery: `select "_dataland_key", "${RECORD_ID}" from "${DATALAND_TABLE_NAME}"`,
  }).response;
  const rows = unpackRows(response);

  const { tableDescriptors } = await history.getCatalogSnapshot({
    logicalTimestamp: transaction.logicalTimestamp - 1,
  }).response;
  const tableDescriptor = tableDescriptors.find(
    (descriptor) => descriptor.tableName === DATALAND_TABLE_NAME
  );
  if (tableDescriptor == null) {
    console.error("Writeback - Could not find table descriptor by table name", {
      tableName: DATALAND_TABLE_NAME,
    });
    return;
  }

  const recordIdMap: Record<number, string> = {};
  for (const row of rows) {
    const key = row["_dataland_key"] as number;
    const recordId = row[RECORD_ID] as string;
    recordIdMap[key] = recordId;
  }

  const fieldNameMap: Record<string, string> = {};
  for (const columnDescriptor of tableDescriptor.columnDescriptors) {
    fieldNameMap[columnDescriptor.columnName] =
      columnDescriptor.columnAnnotations["dataland.io/airtable-field-name"];
  }

  for (const protoMutation of transaction.mutations) {
    const mutation = protoMutation.kind;
    switch (mutation.oneofKind) {
      case "insertRows": {
        if (tableDescriptor.tableName !== mutation.insertRows.tableName) {
          continue;
        }
        await insertRowsWriteback(
          airtableTable,
          mutation,
          fieldNameMap,
          recordIdMap
        );
        break;
      }
      case "updateRows": {
        if (tableDescriptor.tableName !== mutation.updateRows.tableName) {
          continue;
        }
        await updateRowsWriteback(
          airtableTable,
          mutation,
          fieldNameMap,
          recordIdMap
        );
        break;
      }
      case "deleteRows": {
        if (tableDescriptor.tableName !== mutation.deleteRows.tableName) {
          continue;
        }
        await deleteRowsWriteback(airtableTable, mutation, recordIdMap);
        break;
      }
    }
  }
};

registerTransactionHandler(transactionHandler);

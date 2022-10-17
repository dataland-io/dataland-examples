import {
  AirtableImportedRecords as AirtableRecords,
  AIRTABLE_FIELD_NAME,
  AirtableCreateRecord,
  AirtableCreateRecords,
  AirtableDeleteRecords,
  fetchRetry,
  getSyncTargets,
  RECORD_ID,
  syncMappingJsonT,
  SyncTarget,
  UpdateRecord,
  AirtableUpdateRecords,
  validateTableName,
} from "./common";
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
import Airtable from "airtable";

// NOTE(gab): 10 is maximum allowed by airtable
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
  syncTarget: SyncTarget,
  updateRecords: AirtableUpdateRecords
) => {
  const airtableBase = new Airtable({
    apiKey: getEnv("AIRTABLE_API_KEY"),
  }).base(syncTarget.base_id);
  const airtableTable = airtableBase(syncTarget.table_id);

  const updatedRecords: AirtableRecords = [];
  const chunks = chunkAirtablePayload(updateRecords);
  for (const chunk of chunks) {
    try {
      const records = await airtableTable.update(chunk, { typecast: true });
      updatedRecords.push(...records);
    } catch (error) {
      console.error("Writeback - Failed to update Airtable rows", {
        error,
        payload: chunk,
      });
    }
  }
  if (updatedRecords.length !== 0) {
    console.log(
      "Writeback - Successfully updated the following Airtable rows:",
      { recordIds: updatedRecords.map((record) => record.id) }
    );
  }
};

const airtableCreateRows = async (
  syncTarget: SyncTarget,
  createRecords: AirtableCreateRecords
): Promise<string[]> => {
  const airtableBase = new Airtable({
    apiKey: getEnv("AIRTABLE_API_KEY"),
  }).base(syncTarget.base_id);
  const airtableTable = airtableBase(syncTarget.table_id);

  const createdRecords: AirtableRecords = [];
  const chunks = chunkAirtablePayload(createRecords);
  for (const chunk of chunks) {
    try {
      const records = await airtableTable.create(chunk, { typecast: true });
      createdRecords.push(...records);
    } catch (error) {
      console.error("Writeback - Failed to insert Airtable rows", {
        error,
        payload: chunk,
      });
    }
  }
  if (createdRecords.length !== 0) {
    console.log(
      "Writeback - Successfully created the following Airtable rows:",
      { recordIds: createdRecords.map((record) => record.id) }
    );
  }
  return createdRecords.map((record) => record.id);
};

const airtableDestroyRows = async (
  syncTarget: SyncTarget,
  deleteRecordIds: AirtableDeleteRecords
) => {
  const airtableBase = new Airtable({
    apiKey: getEnv("AIRTABLE_API_KEY"),
  }).base(syncTarget.base_id);
  const airtableTable = airtableBase(syncTarget.table_id);

  const destroyedRecords: AirtableRecords = [];
  const chunks = chunkAirtablePayload(deleteRecordIds);
  for (const chunk of chunks) {
    try {
      const records = await airtableTable.destroy(chunk);
      destroyedRecords.push(...records);
    } catch (error) {
      console.error("Writeback - Failed to delete Airtable rows", {
        error,
        payload: chunk,
      });
    }
  }
  if (destroyedRecords.length !== 0) {
    console.log(
      "Writeback - Successfully deleted the following Airtable rows:",
      { recordIds: destroyedRecords.map((record) => record.id) }
    );
  }
};

const insertRowsWriteback = async (
  syncTarget: SyncTarget,
  mutation: Extract<Mutation["kind"], { oneofKind: "insertRows" }>,
  fieldNameMap: Record<string, string>,
  recordIdMap: Record<number, string>
) => {
  const { rows, columnNames } = mutation.insertRows;
  const createRecords: AirtableCreateRecords = [];
  for (let i = 0; i < rows.length; i++) {
    const createRecord: AirtableCreateRecord = { fields: {} };
    const { values: listValues } = rows[i]!;
    if (listValues == null) {
      console.error("Writeback - List value is empty");
      continue;
    }

    const values = listValues.values;
    for (let j = 0; j < values.length; j++) {
      const scalar = valueToScalar(values[j]!);
      // NOTE(gab): Airtable expects no value for empty cells
      if (scalar == null) {
        continue;
      }
      const columnName = columnNames[j]!;
      const fieldName = fieldNameMap[columnName];
      if (fieldName == null) {
        console.error("Writeback - Could not find field name by column name", {
          columnName,
        });
        continue;
      }

      if (!syncTarget.allowed_writeback_field_list.has(fieldName)) {
        console.log(
          "Writeback - Tried to insert a field that does not allow writeback. Skipping value",
          {
            fieldName,
            value: scalar,
          }
        );
        continue;
      }
      createRecord.fields[fieldName] = scalar;
    }
    createRecords.push(createRecord);
  }

  if (createRecords.length === 0) {
    return;
  }
  const recordIds = await airtableCreateRows(syncTarget, createRecords);
  if (recordIds.length !== rows.length) {
    console.error(
      "Writeback - Inserted Dataland rows do not have the same length as the inserted Airtable rows. This will happen if any of the inserted rows failed to insert an Airtable row, otherwise it is an unexpected error. Data state will be reconciled in next Airtable Sync.",
      {
        datalandRowsLength: rows.length,
        airtableRowsLength: recordIds.length,
      }
    );
    return;
  }

  const mutations = new MutationsBuilder();
  for (let i = 0; i < recordIds.length; i++) {
    // NOTE(gab): Record ids are returned from Airtable in the same order
    // as the records were sent, therefore we can safely assume that a index
    // in recordIds corresponds to the same index in the rows.
    const recordId = recordIds[i]!;
    const rowId = rows[i]!.rowId;
    recordIdMap[rowId] = recordId;
    mutations.updateRow(syncTarget.table_name, rowId, {
      [RECORD_ID]: recordId,
    });
  }
  await mutations.run(getDbClient());
};

const updateRowsWriteback = async (
  syncTarget: SyncTarget,
  mutation: Extract<Mutation["kind"], { oneofKind: "updateRows" }>,
  fieldNameMap: Record<string, string>,
  recordIdMap: Record<number, string>
) => {
  const { rows, columnNames } = mutation.updateRows;
  const updateRecords: AirtableUpdateRecords = [];
  for (let i = 0; i < rows.length; i++) {
    const { rowId, values: listValues } = rows[i]!;
    if (listValues == null) {
      console.error("Writeback - List value is empty");
      continue;
    }
    const recordId = recordIdMap[rowId];
    if (recordId == null) {
      console.error(
        "Writeback - Could not find Airtable record id by Dataland rowId",
        { rowId }
      );
      continue;
    }

    const updateRecord: UpdateRecord = { id: recordId, fields: {} };
    const values = listValues.values;
    for (let j = 0; j < values.length; j++) {
      const scalar = valueToScalar(values[j]!);
      const columnName = columnNames[j]!;
      const fieldName = fieldNameMap[columnName];
      if (fieldName == null) {
        console.error(
          "Writeback - Could not find Airtable field name by Dataland column name",
          { columnName }
        );
        continue;
      }
      if (!syncTarget.allowed_writeback_field_list.has(fieldName)) {
        console.log(
          "Writeback - Tried to insert a field that does not allow writeback. Skipping value",
          {
            fieldName,
            value: scalar,
          }
        );
        continue;
      }
      // @ts-ignore - NOTE(gab): Nulls are used to clear ANY value from Airtable.
      // The reason it's not in their type system is probably that they "expect"
      // the empty type for that field: "false", "", [] etc. But since
      // there is no schema from Airtable, the correct "empty type" cannot be known,
      // and null is used.
      updateRecord.fields[fieldName] = scalar ?? null;
    }
    updateRecords.push(updateRecord);
  }

  if (updateRecords.length === 0) {
    return;
  }
  await airtableUpdateRows(syncTarget, updateRecords);
};

const deleteRowsWriteback = async (
  syncTarget: SyncTarget,
  mutation: Extract<Mutation["kind"], { oneofKind: "deleteRows" }>,
  recordIdMap: Record<number, string>
) => {
  const deleteRecordIds: AirtableDeleteRecords = [];
  const rowIds = mutation.deleteRows.rowIds;
  for (const rowId of rowIds) {
    const recordId = recordIdMap[rowId];
    if (recordId == null) {
      console.error(
        "Writeback - Could not find Airtable record id by Dataland row id",
        { rowId }
      );
      continue;
    }
    deleteRecordIds.push(recordId);
  }

  if (deleteRecordIds.length === 0) {
    return;
  }
  await airtableDestroyRows(syncTarget, deleteRecordIds);
};

const transactionHandler = async (transaction: Transaction) => {
  const ALLOW_WRITEBACK_BOOLEAN = getEnv("AIRTABLE_ALLOW_WRITEBACK_BOOLEAN");
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

  const syncTargets = getSyncTargets();
  for (const syncTarget of syncTargets) {
    const datalandTableName = validateTableName(syncTarget.table_name);

    const history = getHistoryClient();
    const response = await history.querySqlSnapshot({
      logicalTimestamp: transaction.logicalTimestamp - 1,
      sqlQuery: `select "_row_id", "${RECORD_ID}" from "${datalandTableName}"`,
    }).response;
    const rows = unpackRows(response);

    const { tableDescriptors } = await history.getCatalogSnapshot({
      logicalTimestamp: transaction.logicalTimestamp - 1,
    }).response;
    const tableDescriptor = tableDescriptors.find(
      (descriptor) => descriptor.tableName === datalandTableName
    );
    if (tableDescriptor == null) {
      console.error(
        "Writeback - Could not find table descriptor. Table might have been deleted.",
        { tableName: datalandTableName }
      );
      return;
    }

    const recordIdMap: Record<number, string> = {};
    for (const row of rows) {
      const rowId = row["_row_id"] as number;
      const recordId = row[RECORD_ID] as string;
      recordIdMap[rowId] = recordId;
    }

    type ColumnName = string;
    type FieldName = string;
    const fieldNameMap: Record<ColumnName, FieldName> = {};
    for (const columnDescriptor of tableDescriptor.columnDescriptors) {
      if (columnDescriptor.columnName === RECORD_ID) {
        continue;
      }
      const fieldName = columnDescriptor.columnAnnotations[AIRTABLE_FIELD_NAME];
      // NOTE(gab): this makes users unable to add arbitrary columns to the table,
      // as they will not have a field name annotation.
      if (fieldName == null) {
        throw new Error(
          "Writeback - Aborting: Missing field name annotation on column descriptor"
        );
      }
      fieldNameMap[columnDescriptor.columnName] = fieldName;
    }

    for (const protoMutation of transaction.mutations) {
      const mutation = protoMutation.kind;
      switch (mutation.oneofKind) {
        case "insertRows": {
          if (tableDescriptor.tableName !== mutation.insertRows.tableName) {
            continue;
          }
          await insertRowsWriteback(
            syncTarget,
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
            syncTarget,
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
          await deleteRowsWriteback(syncTarget, mutation, recordIdMap);
          break;
        }
      }
    }
  }
};

registerTransactionHandler(transactionHandler);

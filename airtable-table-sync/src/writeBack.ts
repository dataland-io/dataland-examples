import {
  AirtableImportedRecords,
  AIRTABLE_FIELD_NAME,
  CreateRecord,
  CreateRecords,
  fetchRetry,
  RECORD_ID,
  UpdateRecord,
  UpdateRecords,
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

const AIRTABLE_MAX_UPDATES = 10;
const chunkAirtablePayload = <T>(payload: T[]) => {
  const chunks: T[][] = [];
  for (let i = 0; i < payload.length; i += AIRTABLE_MAX_UPDATES) {
    const chunk = payload.slice(i, i + AIRTABLE_MAX_UPDATES);
    chunks.push(chunk);
  }
  return chunks;
};

const airtableUpdateRows = async (updateRecords: UpdateRecords) => {
  const baseId = getEnv("AIRTABLE_BASE_ID");
  const tableName = getEnv("AIRTABLE_TABLE_NAME");
  const apiKey = getEnv("AIRTABLE_API_KEY");
  const url = encodeURI(`https://api.airtable.com/v0/${baseId}/${tableName}`);
  const headers = {
    Authorization: `Bearer ${apiKey}`,
    "Content-Type": "application/json",
  };

  const chunks = chunkAirtablePayload(updateRecords);
  for (const chunk of chunks) {
    const body = JSON.stringify({ records: chunk });
    const response = await fetchRetry(() =>
      fetch(url, {
        method: "PATCH",
        headers,
        body,
      })
    );
    if (response === "error") {
      throw new Error(
        `Airtable update records request failed: ${JSON.stringify({
          url,
          body,
        })}`
      );
    }
  }
  console.log(
    "Writeback - Successfully update Airtable records:",
    updateRecords.map((record) => record.id)
  );
};

const airtableCreateRows = async (
  createRecords: CreateRecords
): Promise<string[]> => {
  const baseId = getEnv("AIRTABLE_BASE_ID");
  const tableName = getEnv("AIRTABLE_TABLE_NAME");
  const apiKey = getEnv("AIRTABLE_API_KEY");
  const url = encodeURI(`https://api.airtable.com/v0/${baseId}/${tableName}`);
  const headers = {
    "Content-Type": "application/json",
    Authorization: `Bearer ${apiKey}`,
  };

  const createdRecordIds: string[] = [];
  const chunks = chunkAirtablePayload(createRecords);
  for (const chunk of chunks) {
    const body = JSON.stringify({ records: chunk });
    const response = await fetchRetry(() =>
      fetch(url, {
        method: "POST",
        headers,
        body,
      })
    );
    if (response === "error") {
      throw new Error(
        `Airtable create records request failed: ${JSON.stringify({
          url,
          body,
        })}`
      );
    }

    const json = await response.json();
    const records: AirtableImportedRecords = json.records;
    console.log(records, "HERE");
    const ids = records.map((record) => record.id);
    createdRecordIds.push(...ids);
  }
  console.log(
    "Writeback - Successfully created Airtable records:",
    createdRecordIds
  );
  return createdRecordIds;
};

const airtableDestroyRows = async (deleteRecordIds: string[]) => {
  const baseId = getEnv("AIRTABLE_BASE_ID");
  const tableName = getEnv("AIRTABLE_TABLE_NAME");
  const apiKey = getEnv("AIRTABLE_API_KEY");
  const url = `https://api.airtable.com/v0/${baseId}/${tableName}`;
  const headers = {
    "Content-Type": "application/x-www-form-urlencoded",
    Authorization: `Bearer ${apiKey}`,
  };

  const chunks = chunkAirtablePayload(deleteRecordIds);
  for (const chunk of chunks) {
    const urlEncodedParams = chunk
      .map((recordId) => `records[]=${recordId}`)
      .join("&");
    const uriUrl = encodeURI(`${url}?${urlEncodedParams}`);
    const response = await fetchRetry(() =>
      fetch(uriUrl, {
        method: "DELETE",
        headers,
      })
    );
    if (response === "error") {
      throw new Error(
        `Airtable delete records request failed: ${JSON.stringify({
          url: uriUrl,
        })}`
      );
    }
  }
  console.log(
    "Writeback - Successfully deleted Airtable records:",
    deleteRecordIds
  );
};

const insertRowsWriteback = async (
  mutation: Extract<Mutation["kind"], { oneofKind: "insertRows" }>,
  fieldNameMap: Record<string, string>,
  recordIdMap: Record<number, string>
) => {
  const createRecords: CreateRecords = [];
  const { rows, columnNames } = mutation.insertRows;

  for (let i = 0; i < rows.length; i++) {
    const createRecord: CreateRecord = { fields: {} };
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

      if (scalar != null) {
        createRecord.fields[fieldName] = scalar;
      }
    }

    createRecords.push(createRecord);
  }

  if (createRecords.length === 0) {
    return;
  }
  const recordIds = await airtableCreateRows(createRecords);
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

    // TODO: move out
    mutations.updateRow(getEnv("AIRTABLE_DATALAND_TABLE_NAME"), rowKey, {
      [RECORD_ID]: recordId,
    });
    // mutations.push(update);
  }
  await mutations.run(getDbClient());
};

const updateRowsWriteback = async (
  mutation: Extract<Mutation["kind"], { oneofKind: "updateRows" }>,
  fieldNameMap: Record<string, string>,
  recordIdMap: Record<number, string>
) => {
  const updateRecords: UpdateRecords = [];
  const { rows, columnNames } = mutation.updateRows;
  for (let i = 0; i < rows.length; i++) {
    const { rowId, values: listValues } = rows[i]!;
    if (listValues == null) {
      console.error("List value is empty");
      continue;
    }

    const recordId = recordIdMap[rowId];
    if (recordId == null) {
      console.error("Writeback - Could not find record id by dataland rowId", {
        rowId,
      });
      continue;
    }

    const updateRecord: UpdateRecord = { id: recordId, fields: {} };
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
      // The reason it's not in their type system is probably that they "expect"
      // the empty type for that field: "false", "", [] etc and not null. But since
      // no schema is provided from their side, the correct "empty type" cannot be known,
      // and null is used.
      updateRecord.fields[fieldName] = scalar ?? null;
    }

    updateRecords.push(updateRecord);
  }

  if (updateRecords.length === 0) {
    return;
  }
  await airtableUpdateRows(updateRecords);
};

const deleteRowsWriteback = async (
  mutation: Extract<Mutation["kind"], { oneofKind: "deleteRows" }>,
  recordIdMap: Record<number, string>
) => {
  const deleteRecordIds: string[] = [];
  const rowIds = mutation.deleteRows.rowIds;
  for (const rowId of rowIds) {
    const recordId = recordIdMap[rowId];
    if (recordId == null) {
      console.error("Writeback - Could not find record id by dataland key", {
        rowId,
      });
      continue;
    }
    deleteRecordIds.push(recordId);
  }

  if (deleteRecordIds.length === 0) {
    return;
  }
  await airtableDestroyRows(deleteRecordIds);
};

const transactionHandler = async (transaction: Transaction) => {
  console.log("transactions");
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

  const DATALAND_TABLE_NAME = getEnv("AIRTABLE_DATALAND_TABLE_NAME");

  const history = getHistoryClient();
  const response = await history.querySqlSnapshot({
    logicalTimestamp: transaction.logicalTimestamp - 1,
    sqlQuery: `select "_row_id", "${RECORD_ID}" from "${DATALAND_TABLE_NAME}"`,
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
    const key = row["_row_id"] as number;
    const recordId = row[RECORD_ID] as string;
    recordIdMap[key] = recordId;
  }

  const fieldNameMap: Record<string, string> = {};
  for (const columnDescriptor of tableDescriptor.columnDescriptors) {
    if (columnDescriptor.columnName === "record_id") {
      continue;
    }
    const fieldName = columnDescriptor.columnAnnotations[AIRTABLE_FIELD_NAME];
    // NOTE(gab): this makes users unable to add arbitrary columns to the table,
    // as they will not have a field name annotation.
    if (fieldName == null) {
      throw new Error(
        "Aborting: Missing field name annotation on column descriptor"
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
        await insertRowsWriteback(mutation, fieldNameMap, recordIdMap);
        break;
      }
      case "updateRows": {
        if (tableDescriptor.tableName !== mutation.updateRows.tableName) {
          continue;
        }
        await updateRowsWriteback(mutation, fieldNameMap, recordIdMap);
        break;
      }
      case "deleteRows": {
        if (tableDescriptor.tableName !== mutation.deleteRows.tableName) {
          continue;
        }
        await deleteRowsWriteback(mutation, recordIdMap);
        break;
      }
    }
  }
};

registerTransactionHandler(transactionHandler);

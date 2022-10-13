import { RECORD_ID } from "./constants";
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

import {
  FieldSet as AirtableFieldSet,
  RecordData as AirtableRecordData,
} from "airtable";

const AIRTABLE_MAX_UPDATES = 10;
const chunkAirtablePayload = <T>(payload: T[]) => {
  const chunks: T[][] = [];
  for (let i = 0; i < payload.length; i += AIRTABLE_MAX_UPDATES) {
    const chunk = payload.slice(i, i + AIRTABLE_MAX_UPDATES);
    chunks.push(chunk);
  }
  return chunks;
};

const airtableUpdateRows2 = async (
  updateRecords: AirtableRecordData<Partial<AirtableFieldSet>>[]
) => {
  const baseId = getEnv("AIRTABLE_BASE_ID");
  const tableName = getEnv("AIRTABLE_TABLE_NAME");
  const apiKey = getEnv("AIRTABLE_API_KEY");
  const url = `https://api.airtable.com/v0/${baseId}/${tableName}`;
  const headers = {
    Authorization: `Bearer ${apiKey}`,
    "Content-Type": "application/json",
  };

  const chunks = chunkAirtablePayload(updateRecords);
  for (const chunk of chunks) {
    const resp = await fetch(url, {
      method: "PATCH",
      headers,
      body: JSON.stringify({ records: chunk }),
    });
    if (!resp.ok) {
      throw new Error(`Airtable request failed. Reason: ${resp.statusText}`);
    }
  }
};

const airtableCreateRows = async (
  createRecords: { fields: AirtableFieldSet }[]
): Promise<string[]> => {
  const baseId = getEnv("AIRTABLE_BASE_ID");
  const tableName = getEnv("AIRTABLE_TABLE_NAME");
  const apiKey = getEnv("AIRTABLE_API_KEY");
  const url = `https://api.airtable.com/v0/${baseId}/${tableName}`;
  const headers = {
    "Content-Type": "application/json",
    Authorization: `Bearer ${apiKey}`,
  };

  const removedIds: string[] = [];
  const chunks = chunkAirtablePayload(createRecords);
  for (const chunk of chunks) {
    const resp = await fetch(url, {
      method: "POST",
      headers,
      body: JSON.stringify({ records: chunk }),
    });
    if (!resp.ok) {
      throw new Error(`Airtable request failed. Reason: ${resp.statusText}`);
    }
    const json = await resp.json();
    const ids = json.records.map((record: any) => record.id);
    removedIds.push(...ids);
  }
  return removedIds;
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
    const head = "?records[]=" + chunk.join("&records[]=");

    const resp = await fetch(`${url}${head}`, {
      method: "DELETE",
      headers,
    });
    if (!resp.ok) {
      throw new Error(`Airtable request failed. Reason: ${resp.statusText}`);
    }
  }
};

const insertRowsWriteback = async (
  mutation: Extract<Mutation["kind"], { oneofKind: "insertRows" }>,
  fieldNameMap: Record<string, string>,
  recordIdMap: Record<number, string>
) => {
  const createRecords: { fields: AirtableFieldSet }[] = [];
  const { rows, columnNames } = mutation.insertRows;

  for (let i = 0; i < rows.length; i++) {
    const createRecord: AirtableFieldSet = {};
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

      createRecord[fieldName] = scalar ?? undefined; // NOTE(gab): accepts undefined, not null
    }

    createRecords.push({ fields: createRecord });
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
  const updateRecords: AirtableRecordData<Partial<AirtableFieldSet>>[] = [];
  const { rows, columnNames } = mutation.updateRows;
  for (let i = 0; i < rows.length; i++) {
    const updateRecord: Partial<AirtableFieldSet> = {};
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
      updateRecord[fieldName] = scalar ?? null;
    }

    const recordId = recordIdMap[rowId];
    if (recordId == null) {
      console.error("Writeback - Could not find record id by dataland rowId", {
        rowId,
      });
      continue;
    }
    updateRecords.push({ id: recordId, fields: updateRecord });
  }

  if (updateRecords.length === 0) {
    return;
  }
  await airtableUpdateRows2(updateRecords);
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
console.log("WRITEBACK");
registerTransactionHandler(transactionHandler);

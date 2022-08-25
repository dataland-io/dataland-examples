import {
  Transaction,
  getCatalogSnapshot,
  querySqlSnapshot,
  unpackRows,
  Schema,
  Mutation,
  registerTransactionHandler,
  Uuid,
} from "@dataland-io/dataland-sdk-worker";
import {
  ALLOW_WRITEBACK_BOOLEAN,
  CLIENT_ID,
  DATALAND_TABLE_NAME,
  MINDBODY_API_KEY,
  MINDBODY_AUTHORIZATION,
  MINDBODY_SITE_ID,
  SYNC_TABLES_MARKER,
} from "./constants";

const AIRTABLE_MAX_UPDATES = 10;
const chunkAirtablePayload = <T>(payload: T[]) => {
  const chunks: T[][] = [];

  for (let i = 0; i < payload.length; i += AIRTABLE_MAX_UPDATES) {
    const chunk = payload.slice(i, i + AIRTABLE_MAX_UPDATES);
    chunks.push(chunk);
  }
  return chunks;
};

const postUpdateClient = async (client: Record<string, any>) => {
  console.log("Updating new client with:", client);
  const myHeaders = new Headers();
  myHeaders.append("Content-Type", "application/json");
  myHeaders.append("API-Key", MINDBODY_API_KEY);
  myHeaders.append("SiteId", MINDBODY_SITE_ID);
  myHeaders.append("Authorization", MINDBODY_AUTHORIZATION);

  const raw = JSON.stringify({
    Client: client,
    SendEmail: false,
    CrossRegionalUpdate: false,
    Test: false,
  });

  const requestOptions: RequestInit = {
    method: "POST",
    headers: myHeaders,
    body: raw,
    redirect: "follow",
  };

  try {
    const resp = await fetch(
      "https://api.mindbodyonline.com/public/v6/client/updateclient",
      requestOptions
    );
    const json = await resp.json();
    console.log("Success response from MBO:", json);
  } catch (e) {
    console.error("failed to update client", e);
  }
};

const insertRowsWriteback = async (
  mutation: Extract<Mutation, { kind: "insert_rows" }>,
  schema: Schema,
  columnNameMap: Record<Uuid, string>,
  recordIdMap: Record<number, string>
) => {
  const createRows: { fields: any }[] = [];
  const { rows, columnMapping } = mutation.value;

  for (let i = 0; i < rows.length; i++) {
    const createRow: any = {};
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
};

const updateRowsWriteback = async (
  mutation: Extract<Mutation, { kind: "update_rows" }>,
  columnNameMap: Record<Uuid, string>,
  clientIdMap: Record<number, string>
) => {
  console.log("Handling mutation:", mutation);
  const { rows, columnMapping } = mutation.value;
  for (let i = 0; i < rows.length; i++) {
    const updateClient: Record<string, any> = {};
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

      const value = taggedScalar?.value ?? null;
      const parsedValue = (() => {
        if (typeof value !== "string") {
          return value;
        }
        const isObject = value.startsWith("{") && value.endsWith("}");
        const isArray = value.startsWith("[") && value.endsWith("]");

        if (isObject || isArray) {
          try {
            return JSON.parse(value);
          } catch (e) {
            console.error("Writeback - Failed to parse to JSON", { value });
          }
        }
        return value;
      })();

      if (columnName.includes("/~/")) {
        const [parentPropertyKey, propertyKey] = columnName.split("/~/");

        let parentProperty = updateClient[parentPropertyKey];
        if (parentProperty == null) {
          parentProperty = {};
        }
        parentProperty[propertyKey] = parsedValue;

        updateClient[parentPropertyKey] = parentProperty;
      } else {
        updateClient[columnName] = parsedValue;
      }
    }

    const updateClient2: any = {
      ClientCreditCard: {
        CardHolder: "Mark alskdjf",
        CardType: "Visa",
        CardNumber: "2221007699753343",
        ExpMonth: "11",
        ExpYear: "2031",
      },
    };
    updateClient[CLIENT_ID] = clientIdMap[key];

    await postUpdateClient(updateClient);
  }
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
  // await airtableDestroyRows(airtableTable, deleteRows);
};

const transactionHandler = async (transaction: Transaction) => {
  // NOTE(gab): Updating a cell in Dataland while Airtable import cron is running would
  // cause the imported data from airtable to be outdated. When the syncTables transaction
  // finally goes through, it would set the cell to its previous value which we expect.
  // The discrepancy would then be reconciled in the next sync. But if the transaction handler
  // is triggered on syncTables, the outdated cell change would propagate to Airtable again,
  // permanently reverting the cell update.

  if (SYNC_TABLES_MARKER in transaction.transactionAnnotations) {
    return;
  }

  console.log("New transaction registered", transaction);

  const response = await querySqlSnapshot({
    logicalTimestamp: transaction.logicalTimestamp - 1,
    sqlQuery: `select "_dataland_key", "${CLIENT_ID}" from "${DATALAND_TABLE_NAME}"`,
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

  const clientIdMap: Record<number, string> = {};
  for (const row of rows) {
    const key = row["_dataland_key"] as number;
    const clientMap = row[CLIENT_ID] as string;
    clientIdMap[key] = clientMap;
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
        // await insertRowsWriteback(mutation, schema, columnNameMap, clientIdMap);
        break;
      }
      case "update_rows": {
        await updateRowsWriteback(mutation, columnNameMap, clientIdMap);
        break;
      }
      case "delete_rows": {
        // await deleteRowsWriteback(mutation, clientIdMap);
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

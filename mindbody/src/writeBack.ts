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
  MINDBODY_ALLOW_WRITEBACK_BOOLEAN,
  CLIENT_ID,
  DATALAND_CLIENTS_TABLE_NAME,
  MINDBODY_API_KEY,
  MINDBODY_AUTHORIZATION,
  MINDBODY_SITE_ID,
  SYNC_TABLES_MARKER,
} from "./constants";

interface PostUpdateClientResponse {
  ok: boolean;
  message: string;
  client?: Record<string, any>;
}
export const postUpdateClient = async (
  client: Record<string, any>
): Promise<PostUpdateClientResponse> => {
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
    return {
      ok: resp.ok,
      message: `${resp.status}: ${resp.statusText}`,
      client: json.Client,
    };
  } catch (e) {
    if (e instanceof Error) {
      return {
        ok: false,
        message: `Updating Client Error: ${e.name} - ${e.message}`,
      };
    }
    return {
      ok: false,
      message: `UUpdating Client Error: Unexpected error - ${e}`,
    };
  }
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
    sqlQuery: `select "_dataland_key", "${CLIENT_ID}" from "${DATALAND_CLIENTS_TABLE_NAME}"`,
  });
  const rows = unpackRows(response);

  const { tableDescriptors } = await getCatalogSnapshot({
    logicalTimestamp: transaction.logicalTimestamp - 1,
  });
  const tableDescriptor = tableDescriptors.find(
    (descriptor) => descriptor.tableName === DATALAND_CLIENTS_TABLE_NAME
  );
  if (tableDescriptor == null) {
    console.error("Writeback - Could not find table descriptor by table name", {
      tableName: DATALAND_CLIENTS_TABLE_NAME,
    });
    return;
  }

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
        // await updateRowsWriteback(mutation, columnNameMap, clientIdMap);
        break;
      }
      case "delete_rows": {
        // await deleteRowsWriteback(mutation, clientIdMap);
        break;
      }
    }
  }
};

if (
  MINDBODY_ALLOW_WRITEBACK_BOOLEAN !== "true" &&
  MINDBODY_ALLOW_WRITEBACK_BOOLEAN !== "false"
) {
  console.error(
    `Writeback - 'ALLOW_WRITEBACK_BOOLEAN' invalid value '${MINDBODY_ALLOW_WRITEBACK_BOOLEAN}', expected 'true' or 'false'.`
  );
}

if (MINDBODY_ALLOW_WRITEBACK_BOOLEAN === "true") {
  registerTransactionHandler(transactionHandler, {
    filterTransactions: "handle-all",
  });
}

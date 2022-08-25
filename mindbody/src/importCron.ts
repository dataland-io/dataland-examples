import { tableFromJSON, tableToIPC } from "@apache-arrow/es2015-esm";
import {
  registerCronHandler,
  Scalar,
  SyncTable,
  syncTables,
} from "@dataland-io/dataland-sdk-worker";
import {
  CLIENT_ID,
  DATALAND_TABLE_NAME,
  MINDBODY_API_KEY,
  MINDBODY_AUTHORIZATION,
  MINDBODY_SITE_ID,
  SYNC_TABLES_MARKER,
} from "./constants";

type ParsedClient = Record<string, Scalar>;
//
const fetchData = async () => {
  const myHeaders = new Headers();
  myHeaders.append("Content-Type", "application/json");
  myHeaders.append("API-Key", MINDBODY_API_KEY);
  myHeaders.append("SiteId", MINDBODY_SITE_ID);
  myHeaders.append("Authorization", MINDBODY_AUTHORIZATION);

  const requestOptions: RequestInit = {
    method: "GET",
    headers: myHeaders,
    redirect: "follow",
  };

  let clients;
  try {
    const resp = await fetch(
      "https://api.mindbodyonline.com/public/v6/client/clients?limit=10&offset=0",
      requestOptions
    );
    const res = await resp.json();
    clients = res.Clients;
  } catch (error) {
    console.error("Error fetching data", error);
    return;
  }

  const fieldNames: Set<string> = new Set();
  for (const client of clients) {
    for (const key in client) {
      const v = client[key];

      if (!Array.isArray(v) && typeof v === "object") {
        for (const key2 in client) {
          fieldNames.add(`${key}/~/${key2}`);
        }
      }

      fieldNames.add(key);
    }
  }

  const parsedClients: ParsedClient[] = [];
  for (const client of clients) {
    const parsedClient: ParsedClient = {};
    for (const key in client) {
      const value = client[key];

      if (Array.isArray(value)) {
        parsedClient[key] = JSON.stringify(value);
        continue;
      }

      if (typeof value === "object") {
        for (const valueKey in value) {
          const key2 = `${key}/~/${valueKey}`;

          const val2 = value[valueKey];
          const val = (() => {
            if (typeof val2 === "object") {
              return JSON.stringify(val2);
            }
            return val2;
          })();

          parsedClient[key2] = val;
        }
        continue;
      }

      parsedClient[key] = value;
    }

    for (const columnName of fieldNames) {
      if (columnName in parsedClient) {
        continue;
      }
      parsedClient[columnName] = null;
    }

    parsedClients.push(parsedClient);
  }

  return parsedClients;
};

const cronHandler = async () => {
  const records = await fetchData();
  if (records == null) {
    return;
  }

  const table = tableFromJSON(records);
  const batch = tableToIPC(table);

  const syncTable: SyncTable = {
    tableName: DATALAND_TABLE_NAME,
    arrowRecordBatches: [batch],
    identityColumnNames: [CLIENT_ID],
  };

  await syncTables({
    syncTables: [syncTable],
    transactionAnnotations: {
      [SYNC_TABLES_MARKER]: "true",
    },
  });
  console.log("Sync done");
};

registerCronHandler(cronHandler);

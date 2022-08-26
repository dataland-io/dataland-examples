import { tableFromJSON, tableToIPC } from "@apache-arrow/es2015-esm";
import {
  registerCronHandler,
  Scalar,
  SyncTable,
  syncTables,
} from "@dataland-io/dataland-sdk-worker";
import {
  CLIENT_ID,
  DATALAND_CLIENTS_TABLE_NAME,
  MINDBODY_API_KEY,
  MINDBODY_AUTHORIZATION,
  MINDBODY_SITE_ID,
  SYNC_TABLES_MARKER,
} from "./constants";

type ParsedClient = Record<string, Scalar>;

const parseValue = (k: string, v: Scalar) => {
  if (v == null) {
    return v;
  }

  // NOTE(gab): docs says it's a string but have only seen numbers. adding check to make sure
  if (k === "MobileProvider" && typeof v !== "number") {
    console.error(
      "Import - MobileProvider is the wrong data type, should be a number",
      {
        MobileProvider: v,
      }
    );
    return null;
  }

  if (typeof v === "object") {
    return JSON.stringify(v);
  }
  return v;
};

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
      "https://api.mindbodyonline.com/public/v6/client/clients?limit=5&offset=0",
      requestOptions
    );
    const res = await resp.json();
    clients = res.Clients;
    if (clients == null) {
      console.error("Clients is null for no reason?", res);
      return;
    }
  } catch (error) {
    console.error("Error fetching data", error);
    return;
  }

  const columnNames: Set<string> = new Set();
  for (const client of clients) {
    for (const key in client) {
      const v = client[key];

      if (!Array.isArray(v) && typeof v === "object") {
        for (const key2 in v) {
          columnNames.add(`${key}/~/${key2}`);
        }
        continue;
      }

      columnNames.add(key);
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
          parsedClient[key2] = parseValue(key2, value[valueKey]);
        }
        continue;
      }
      parsedClient[key] = parseValue(key, value);
    }

    for (const columnName of columnNames) {
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
  console.log("cron");
  const records = await fetchData();
  if (records == null) {
    return;
  }

  const d: any = {};
  records.forEach((record) => {
    for (const k in record) {
      const v = record[k];
      if (d[k] == null) {
        d[k] = [];
      }
      d[k].push({ v });
    }
  });
  // console.log("TYPPPES", d);

  const table = tableFromJSON(records);
  const batch = tableToIPC(table);

  const syncTable: SyncTable = {
    tableName: DATALAND_CLIENTS_TABLE_NAME,
    arrowRecordBatches: [batch],
    identityColumnNames: [CLIENT_ID],
    keepExtraColumns: true,
  };

  console.log("befoere sync");
  const transaction = await syncTables({
    syncTables: [syncTable],

    transactionAnnotations: {
      [SYNC_TABLES_MARKER]: "true",
    },
  });
};

console.log("ref");
registerCronHandler(cronHandler);

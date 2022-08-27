import { tableFromJSON, tableToIPC } from "@apache-arrow/es2015-esm";
import {
  registerCronHandler,
  Scalar,
  SyncTable,
  syncTables,
} from "@dataland-io/dataland-sdk-worker";
import { ClientGet, clientGetT } from "./client";
import {
  CLIENT_ID,
  DATALAND_CLIENTS_TABLE_NAME,
  MINDBODY_API_KEY,
  MINDBODY_AUTHORIZATION,
  MINDBODY_SITE_ID,
  SYNC_TABLES_MARKER,
} from "./constants";

type ParsedClient = Record<string, Scalar>;

const parseValue = (k: string, v: any) => {
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

export const parseClients = (clients: ClientGet[]) => {
  const issues: any = [];
  for (const client of clients) {
    const res = clientGetT.safeParse(client);
    if (res.success === false) {
      for (const issue of res.error.issues) {
        console.error(issue);
        issues.push(issue);
      }
    }
  }

  const columnNames: Set<string> = new Set();
  for (const client of clients) {
    for (const key in client) {
      const v = client[key as keyof typeof client];

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
      const value = client[key as keyof ClientGet];

      if (typeof value == null) {
        parsedClient[key] = value as null;
        continue;
      }

      if (Array.isArray(value)) {
        parsedClient[key] = JSON.stringify(value);
        continue;
      }

      if (typeof value === "object") {
        for (const valueKey in value) {
          const key2 = `${key}/~/${valueKey}`;
          parsedClient[key2] = parseValue(
            key2,
            value[valueKey as keyof typeof value]
          );
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

export const fetchClients = async (opts?: { clientId?: string }) => {
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

  const getUrl = (offset: number) => {
    let url = `https://api.mindbodyonline.com/public/v6/client/clients?limit=200&offset=${offset}`;
    const clientId = opts?.clientId;
    if (clientId != null) {
      url = `${url}?clientIDs=${clientId}`;
    }
    return url;
  };

  const clients: ClientGet[] = [];

  const onePage = async (offset: number) => {
    const url = getUrl(offset);

    try {
      const resp = await fetch(url, requestOptions);
      const res = await resp.json();
      const pageClients = res.Clients;
      console.log(res);
      if (pageClients == null) {
        throw new Error("Clients is null for no reason?", res);
      }

      clients.push(pageClients);
      await onePage(offset + 200);
    } catch (error) {
      console.error("Error fetching data", error);
      return;
    }
  };

  return parseClients(clients);
};

const cronHandler = async () => {
  console.log("cron");
  const records = await fetchClients();
  if (records == null) {
    return;
  }

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

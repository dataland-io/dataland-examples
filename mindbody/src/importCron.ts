import { tableFromJSON, tableToIPC } from "@apache-arrow/es2015-esm";
import {
  registerCronHandler,
  SyncTable,
  syncTables,
} from "@dataland-io/dataland-sdk-worker";
import { Client } from "./client";
import {
  CLIENT_ID,
  DATALAND_CLIENTS_TABLE_NAME,
  MINDBODY_API_KEY,
  MINDBODY_AUTHORIZATION,
  MINDBODY_REQUEST_LIMIT,
  MINDBODY_SITE_ID,
  SYNC_TABLES_MARKER,
} from "./constants";
import createFetchRetry from "fetch-retry-ts";
import { parseClients } from "./parse";

const fetchRetry = createFetchRetry(fetch, {
  retries: 3,
  retryDelay: (attempt) => {
    return Math.pow(2, attempt) * 1000;
  },
  retryOn: (attempts, retries, error, response) => {
    if (attempts !== 0) {
      const errMsg = (() => {
        if (error != null) {
          return "network error";
        }
        if (response != null) {
          return `${response.status}: ${response.statusText}`;
        }
        return "Unknown";
      })();
      console.log(error, response);
      console.error(
        `Import - Clients fetch failed, retrying attempt: ${attempts}. Message: ${errMsg}`
      );
    }

    const isRetry = attempts < retries;
    const isError = error != null || response == null || response.status >= 400;
    return isRetry && isError;
  },
});

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
    let url = `https://api.mindbodyonline.com/public/v6/client/clients?limit=${MINDBODY_REQUEST_LIMIT}&offset=${offset}`;
    const clientId = opts?.clientId;
    if (clientId != null) {
      url = `${url}&clientIDs=${clientId}`;
    }
    return url;
  };

  const clients: Client[] = [];
  const fetchOnePage = async (offset: number) => {
    const url = getUrl(offset);
    const resp = await fetchRetry(url, requestOptions);
    const json = await resp.json();
    const pageClients = json.Clients;
    if (pageClients == null) {
      throw new Error(
        `Failed to fetch clients - ${resp.status}: ${resp.statusText}`
      );
    }
    clients.push(...pageClients);
    console.log("Fetched clients:", clients.length);

    const requestedOffset = json["PaginationResponse"]["RequestedOffset"];
    const totalResults = json["PaginationResponse"]["TotalResults"];
    const newOffset = requestedOffset + MINDBODY_REQUEST_LIMIT;
    if (newOffset >= totalResults) {
      return;
    }
    await fetchOnePage(newOffset);
  };

  await fetchOnePage(0);
  return parseClients(clients);
};

const cronHandler = async () => {
  console.log("cron");
  const records = await fetchClients();
  if (records == null) {
    return;
  }
  if (records.length === 0) {
    console.log("Import - Found 0 clients");
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

  console.log("Transaction from sync", transaction.transactions);
};

console.log("ref");
registerCronHandler(cronHandler);

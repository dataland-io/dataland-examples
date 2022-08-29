import {
  Transaction,
  querySqlSnapshot,
  unpackRows,
  getCatalogSnapshot,
  Schema,
  registerTransactionHandler,
  Mutation,
  runMutations,
} from "@dataland-io/dataland-sdk-worker";
import {
  DATALAND_CLIENTS_TABLE_NAME,
  MINDBODY_API_KEY,
  MINDBODY_AUTHORIZATION,
  MINDBODY_SITE_ID,
} from "./constants";

import { isString, isNumber } from "lodash-es";

const fetchMindbodyClient = async (client_id: string) => {
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
  const url =
    "https://api.mindbodyonline.com/public/v6/client/clients?clientIDs=" +
    client_id;
  console.log("F url:", url);

  const response = await fetch(url, requestOptions);
  const result = await response.json();
  return result;
};

const handler = async (transaction: Transaction) => {
  console.log("xx hi");
  const { tableDescriptors } = await getCatalogSnapshot({
    logicalTimestamp: transaction.logicalTimestamp,
  });

  const schema = new Schema(tableDescriptors);

  const affectedRows = schema.getAffectedRows(
    DATALAND_CLIENTS_TABLE_NAME,
    "MBO Pull",
    transaction
  );

  const lookupKeys: number[] = [];
  for (const [key, value] of affectedRows) {
    if (typeof value === "number") {
      lookupKeys.push(key);
      console.log("key noticed: ", key);
    }
  }

  if (lookupKeys.length === 0) {
    console.log("No lookup keys found");
    return;
  }
  const keyList = `(${lookupKeys.join(",")})`;
  console.log("keyList: ", keyList);

  console.log("xx - Dataland client table name", DATALAND_CLIENTS_TABLE_NAME);
  console.log(
    "xx - query ",
    `select
  _dataland_key, Id
from "${DATALAND_CLIENTS_TABLE_NAME}"
where _dataland_key in ${keyList}`
  );

  const response = await querySqlSnapshot({
    logicalTimestamp: transaction.logicalTimestamp,
    sqlQuery: `select
      _dataland_key, Id
    from "${DATALAND_CLIENTS_TABLE_NAME}" 
    where _dataland_key in ${keyList}`,
  });

  console.log("response: ", response);
  const rows = unpackRows(response);

  if (rows == null) {
    return;
  }

  const mutations: Mutation[] = [];
  for (const row of rows) {
    const client_id = row.Id;

    if (!isString(client_id)) {
      continue;
    }

    const key = row._dataland_key;

    if (!isNumber(key)) {
      continue;
    }

    const client_update = await fetchMindbodyClient(client_id);
    const client_lastname = client_update.Clients[0].LastName;
    const client_firstname = client_update.Clients[0].FirstName;

    for (const client of client_update.Clients) {
      console.log("xx client: ", client.FirstName + " " + client.LastName);
    }

    const update = schema.makeUpdateRows(DATALAND_CLIENTS_TABLE_NAME, key, {
      LastName: client_lastname,
      FirstName: client_firstname,
    });

    if (update == null) {
      continue;
    }
    mutations.push(update);
  }

  await runMutations({ mutations });
};

registerTransactionHandler(handler);

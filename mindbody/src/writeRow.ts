import {
  Transaction,
  querySqlSnapshot,
  unpackRows,
  getCatalogSnapshot,
  Schema,
  Scalar,
  registerTransactionHandler,
  Mutation,
  runMutations,
  wait,
} from "@dataland-io/dataland-sdk-worker";
import { Client, ClientPost } from "./client";
import {
  DATALAND_CLIENTS_TABLE_NAME,
  MINDBODY_API_KEY,
  MINDBODY_AUTHORIZATION,
  MINDBODY_SITE_ID,
} from "./constants";
import { fetchClients } from "./importCron";
import { getClientPost } from "./parse";

interface PostUpdateClientResponseSuccess {
  ok: true;
  message: string;
  client: Client;
}

interface PostUpdateClientResponseError {
  ok: false;
  message: string;
}

type PostUpdateClientResponse =
  | PostUpdateClientResponseSuccess
  | PostUpdateClientResponseError;

const postUpdateClient = async (
  client: ClientPost
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

    console.log("response from MBO:", json);
    const error = json["Error"];
    if (error != null) {
      return {
        ok: false,
        // NOTE(gab): if a cell is incorrect, such as setting a location ID that does not exist,
        // mindbody still responds with 200 success, but the payload is an error.
        message: `400: ${error.Message}`,
      };
    }

    const client = json["Client"];
    if (client != null) {
      return {
        ok: resp.ok,
        message: `${resp.status}: ${resp.statusText}`,
        client,
      };
    }

    // TODO(gab): this should not happen
    console.error("Write - Unexpected post response", JSON.stringify(json));
    throw new Error(`Unexpected post response, contact a developer`);
  } catch (e) {
    if (e instanceof Error) {
      return {
        ok: false,
        message: `Updating Client Error: ${e.name} - ${e.message}`,
      };
    }
    return {
      ok: false,
      message: `Updating Client Error: Unexpected error - ${e}`,
    };
  }
};

const transactionHandler = async (transaction: Transaction) => {
  const { tableDescriptors } = await getCatalogSnapshot({
    logicalTimestamp: transaction.logicalTimestamp - 1,
  });

  const schema = new Schema(tableDescriptors);

  const affectedRows: Map<number, Scalar> = schema.getAffectedRows(
    DATALAND_CLIENTS_TABLE_NAME,
    "MBO Push",
    transaction
  );

  if (affectedRows.size === 0) {
    console.log("ignoring transactions");
    return;
  }

  const updateRowKeys: number[] = [];
  for (const [key, value] of affectedRows) {
    if (typeof value === "number" && value > 0) {
      updateRowKeys.push(key);
    }
  }

  const keyList = `(${updateRowKeys.join(",")})`;
  const response = await querySqlSnapshot({
    logicalTimestamp: transaction.logicalTimestamp - 1,
    sqlQuery: `select * from "${DATALAND_CLIENTS_TABLE_NAME}" where "_dataland_key" in ${keyList}`,
  });

  const rows = unpackRows(response);

  const mutations: Mutation[] = [];
  for (const row of rows) {
    const {
      _dataland_key,
      _dataland_ordinal,
      "MBO push status": _1,
      "MBO pushed at": _2,
      "MBO push": _3,
      ...mbRow
    } = row;
    const key = _dataland_key as number;

    const clientPostResp = getClientPost(mbRow);

    let values: Record<string, Scalar>;
    if (clientPostResp.success === true) {
      const resp = await postUpdateClient(clientPostResp.data);
      values = {
        "MBO push status": resp.message,
        "MBO pushed at": new Date().toISOString(),
      };

      const clients = await fetchClients({ clientId: clientPostResp.data.Id });
      if (clients != null && clients.length === 1) {
        values = { ...values, ...clients[0]! };
      } else {
        console.error("Could not find updated client");
      }

      // values = getDatalandWriteback(resp.message, client);
    } else {
      console.error(
        `Write - Incorrect data types on client: ${JSON.stringify(
          clientPostResp.error.issues
        )}`
      );
      values = {
        "MBO push status": `Incorrect data types on client: ${JSON.stringify(
          clientPostResp.error.issues
        )}`,
        "MBO pushed at": new Date().toISOString(),
      };
    }

    const mutation = schema.makeUpdateRows(
      DATALAND_CLIENTS_TABLE_NAME,
      key,
      values
    );

    mutations.push(mutation);
  }

  if (mutations.length === 0) {
    return;
  }

  await runMutations({ mutations });
};

registerTransactionHandler(transactionHandler);

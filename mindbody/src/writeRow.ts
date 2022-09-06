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
  assertNever,
} from "@dataland-io/dataland-sdk-worker";
import {
  AddClient,
  addClientT,
  Client,
  UpdateClient,
  updateClientT,
} from "./client";
import {
  DATALAND_CLIENTS_TABLE_NAME,
  MINDBODY_API_KEY,
  MINDBODY_AUTHORIZATION,
  MINDBODY_SITE_ID,
} from "./constants";
import { fetchClients } from "./importCron";
import { getClient } from "./parse";

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

interface PostUpdate {
  type: "update";
  client: UpdateClient;
}

interface PostAdd {
  type: "add";
  client: AddClient;
}

type Post = PostUpdate | PostAdd;

const postClient = async (post: Post): Promise<PostUpdateClientResponse> => {
  const type = post.type;

  console.log("Updating new client with:", post.client);
  const myHeaders = new Headers();
  myHeaders.append("Content-Type", "application/json");
  myHeaders.append("API-Key", MINDBODY_API_KEY);
  myHeaders.append("SiteId", MINDBODY_SITE_ID);
  myHeaders.append("Authorization", MINDBODY_AUTHORIZATION);

  const body = () => {
    if (type === "update") {
      return {
        Client: post.client,
        SendEmail: false,
        CrossRegionalUpdate: false,
        Test: false,
      };
    } else if (type === "add") {
      return {
        // Client: post.client,
        ...post.client,
        Test: false,
        SendEmail: false,
        // CrossRegionalUpdate: false,
      };
    }
    assertNever(type);
  };

  const raw = JSON.stringify(body());

  const requestOptions: RequestInit = {
    method: "POST",
    headers: myHeaders,
    body: raw,
    redirect: "follow",
  };

  const endpoint = (() => {
    if (type === "add") {
      return "addclient";
    } else if (type === "update") {
      return "updateclient";
    }
    assertNever(type);
  })();

  try {
    const resp = await fetch(
      `https://api.mindbodyonline.com/public/v6/client/${endpoint}`,
      requestOptions
    );
    const json = await resp.json();

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

const getRowDiff = (
  newRow: Record<string, Scalar>,
  oldRow: Record<string, Scalar>
) => {
  const updatedValues: Record<string, Scalar> = {};
  for (const newKey in newRow) {
    const newValue = newRow[newKey]!;
    const oldValue = oldRow[newKey];

    if (newValue !== oldValue) {
      updatedValues[newKey] = newValue;
    }
  }
  return updatedValues;
};

const updateClient = async (
  row: Record<string, Scalar>,
  prevRowMap: Record<number, Record<string, Scalar>>
): Promise<Record<string, Scalar>> => {
  const { _dataland_key, "MBO push": mboPushValue, Id } = row;
  const key = _dataland_key as number;

  // NOTE(gab): Only updated columns should be posted.
  const prevRow = prevRowMap[key];
  let rowDiff: Record<string, Scalar>;
  if (prevRow != null) {
    rowDiff = getRowDiff(row, prevRow);
    // NOTE(gab): Id is required to identify the client.
    // It will never change and therefore need to be explicitly added after the diff.
    rowDiff["Id"] = row["Id"];
  } else {
    rowDiff = row;
  }

  // NOTE(gab): no need to explicitly remove columns from dataland that should not be
  // posted to mbo - zod takes care of removing any unknown columns.

  const clientPost = getClient(row);
  const clientPostResp = updateClientT.safeParse(clientPost);
  if (clientPostResp.success === false) {
    console.error(
      `Write - Invalid data types on updating client. Issues: ${JSON.stringify(
        clientPostResp.error.issues
      )}`
    );
    return {
      "MBO push status": `(${mboPushValue}) Incorrect data types on client: ${JSON.stringify(
        clientPostResp.error.issues
      )}`,
      "MBO pushed at": new Date().toISOString(),
    };
  }

  const resp = await postClient({
    type: "update",
    client: clientPostResp.data,
  });
  const clients = await fetchClients({ clientId: clientPostResp.data.Id });
  const client = clients[0];
  if (client == null) {
    return {
      "MBO push status": `(${mboPushValue}) ${resp.message}. (warn: could not find updated client)`,
      "MBO pushed at": new Date().toISOString(),
    };
  }

  return {
    "MBO push status": `(${mboPushValue}) ${resp.message}`,
    "MBO pushed at": new Date().toISOString(),
    ...client,
  };
};

const addClient = async (row: Record<string, Scalar>) => {
  const { "MBO push": mboPushValue } = row;

  // NOTE(gab): no need to explicitly remove columns from dataland that should not be
  // posted to mbo - zod takes care of removing any unknown columns.

  const noNull: any = {};
  for (const key in row) {
    const value = row[key];
    const nan = typeof value === "number" && isNaN(value);
    if (value !== false && value !== "" && value != null && !nan) {
      noNull[key] = value;
    }
  }

  const clientPost = getClient(noNull);
  const clientPostResp = addClientT.safeParse(clientPost);
  if (clientPostResp.success !== true) {
    console.error(
      `Write - Invalid data types on adding client. Issues: ${JSON.stringify(
        clientPostResp.error.issues
      )}`
    );
    return {
      "MBO push status": `(${mboPushValue}) Incorrect data types on client: ${JSON.stringify(
        clientPostResp.error.issues
      )}`,
      "MBO pushed at": new Date().toISOString(),
    };
  }

  const resp = await postClient({
    type: "add",
    client: clientPostResp.data,
  });
  if (resp.ok === false) {
    return {
      "MBO push status": `(${mboPushValue}) ${resp.message}`,
      "MBO pushed at": new Date().toISOString(),
    };
  }

  const clients = await fetchClients({ clientId: resp.client.Id });
  const client = clients[0];
  if (client == null) {
    return {
      "MBO push status": `(${mboPushValue}) ${resp.message}. (warn: could not find updated client)`,
      "MBO pushed at": new Date().toISOString(),
    };
  }

  return {
    "MBO push status": `(${mboPushValue}) ${resp.message}`,
    "MBO pushed at": new Date().toISOString(),
    ...client,
  };
};

const transactionHandler = async (transaction: Transaction) => {
  const { tableDescriptors } = await getCatalogSnapshot({
    logicalTimestamp: transaction.logicalTimestamp - 1,
  });

  const schema = new Schema(tableDescriptors);

  const affectedRows: Map<number, Scalar> = schema.getAffectedRows(
    DATALAND_CLIENTS_TABLE_NAME,
    "MBO push",
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
  const responsePromise = querySqlSnapshot({
    logicalTimestamp: transaction.logicalTimestamp,
    sqlQuery: `select * from "${DATALAND_CLIENTS_TABLE_NAME}" where "_dataland_key" in ${keyList}`,
  });
  const prevResponsePromise = querySqlSnapshot({
    logicalTimestamp: transaction.logicalTimestamp - 1,
    sqlQuery: `select * from "${DATALAND_CLIENTS_TABLE_NAME}" where "_dataland_key" in ${keyList}`,
  });

  const [response, prevResponse] = await Promise.all([
    responsePromise,
    prevResponsePromise,
  ]);

  const rows = unpackRows(response);
  const prevRows = unpackRows(prevResponse);
  const prevRowMap: Record<number, Record<string, Scalar>> = {};
  for (const row of prevRows) {
    const key = row["_dataland_key"]! as number;
    prevRowMap[key] = row;
  }

  const mutations: Mutation[] = [];
  for (const row of rows) {
    const key = row["_dataland_key"] as number;

    // NOTE(gab): Id is a read-only column. If a row is added in the dataland UI and
    // the id column is null and the update button is triggered, the client will instead be
    // ADDED rather than UPDATED.
    const clientId = row["Id"] as string;
    const isClientExist = clientId !== "";
    let writeBack;
    if (isClientExist) {
      writeBack = await updateClient(row, prevRowMap);
    } else {
      writeBack = await addClient(row);
    }

    const mutation = schema.makeUpdateRows(
      DATALAND_CLIENTS_TABLE_NAME,
      key,
      writeBack
    );

    mutations.push(mutation);
  }

  if (mutations.length === 0) {
    return;
  }

  await runMutations({ mutations });
};

registerTransactionHandler(transactionHandler);

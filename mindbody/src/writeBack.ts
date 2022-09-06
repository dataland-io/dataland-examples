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
  getEnv,
} from "@dataland-io/dataland-sdk-worker";
import {
  AddClient,
  addClientT,
  Client,
  UpdateClient,
  updateClientT,
} from "./client";
import { fetchClients } from "./importCron";
import { clientToMboRepresentation, parseRow } from "./parse";

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
  myHeaders.append("API-Key", getEnv("MINDBODY_API_KEY"));
  myHeaders.append("SiteId", getEnv("MINDBODY_SITE_ID"));
  myHeaders.append("Authorization", getEnv("MINDBODY_AUTHORIZATION"));

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
  logicalTimestamp: number
): Promise<Record<string, Scalar>> => {
  const { _dataland_key, "MBO push": mboPushValue, Id } = row;
  const key = _dataland_key as number;

  const lastLogicalTimestamp = row["MBO data logical timestamp"] as
    | number
    | null;

  let prevRow: Record<string, Scalar> | null = null;
  if (lastLogicalTimestamp != null) {
    const response = await querySqlSnapshot({
      logicalTimestamp: lastLogicalTimestamp,
      sqlQuery: `select * from "${getEnv(
        "DATALAND_CLIENTS_TABLE_NAME"
      )}" where "_dataland_key" = ${_dataland_key}`,
    });
    prevRow = unpackRows(response).map(parseRow)[0];
    if (prevRow == null) {
      console.error("could not find previous row");
      throw new Error("Could not find previous row");
    }
  }

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

  const clientPost = clientToMboRepresentation(rowDiff);
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
      // TODO: FIGURE OUT IF THIS IS SAFE! can there be a UI transaction taking the
      // next logical timestamp before this transaction is committed?
      // in that case, would need to add another transaction handler for when logical timestamp
      // is added, and make sure timestamps match up etc. or even better add it on ID
      "MBO data logical timestamp": logicalTimestamp + 1,
    };
  }

  return {
    ...client,
    "MBO push status": `(${mboPushValue}) ${resp.message}`,
    "MBO pushed at": new Date().toISOString(),
    "MBO data logical timestamp": logicalTimestamp + 1,
  };
};

const addClient = async (row: Record<string, Scalar>) => {
  const { "MBO push": mboPushValue } = row;

  const noNull: any = {};
  for (const key in row) {
    const value = row[key];
    if (value !== false && value !== "" && value != null) {
      noNull[key] = value;
    }
  }

  const clientPost = clientToMboRepresentation(noNull);
  // NOTE(gab): no need to explicitly remove columns from dataland that should not be
  // posted to mbo - zod takes care of removing any unknown columns.

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

const pushRow = async (
  pushRows: Map<number, Scalar>,
  logicalTimestamp: number,
  schema: Schema
) => {
  const pushRowKeys: number[] = [];
  for (const [key, value] of pushRows) {
    if (typeof value === "number" && value > 0) {
      pushRowKeys.push(key);
    }
  }

  const keyList = `(${pushRowKeys.join(",")})`;

  const response = await querySqlSnapshot({
    logicalTimestamp: logicalTimestamp,
    sqlQuery: `select * from "${getEnv(
      "DATALAND_CLIENTS_TABLE_NAME"
    )}" where "_dataland_key" in ${keyList}`,
  });

  const rows = unpackRows(response).map(parseRow);

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
      writeBack = await updateClient(row, logicalTimestamp);
    } else {
      writeBack = await addClient(row);
    }

    const mutation = schema.makeUpdateRows(
      getEnv("DATALAND_CLIENTS_TABLE_NAME"),
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

const pullRow = async (
  pushRows: Map<number, Scalar>,
  logicalTimestamp: number,
  schema: Schema
) => {
  const pushRowKeys: number[] = [];
  for (const [key, value] of pushRows) {
    if (typeof value === "number" && value > 0) {
      pushRowKeys.push(key);
    }
  }

  const keyList = `(${pushRowKeys.join(",")})`;

  const response = await querySqlSnapshot({
    logicalTimestamp: logicalTimestamp,
    sqlQuery: `select * from "${getEnv(
      "DATALAND_CLIENTS_TABLE_NAME"
    )}" where "_dataland_key" in ${keyList}`,
  });

  const rows = unpackRows(response).map(parseRow);

  const mutations: Mutation[] = [];
  for (const row of rows) {
    const key = row["_dataland_key"] as number;
    const clientId = row["Id"] as string;
    const clients = await fetchClients({ clientId });
    const client = clients[0];
    if (client == null) {
      console.error("Could not pull client");
    }

    const mutation = schema.makeUpdateRows(
      getEnv("DATALAND_CLIENTS_TABLE_NAME"),
      key,
      { ...client, "MBO data logical timestamp": logicalTimestamp + 1 }
    );
    mutations.push(mutation);
  }

  await runMutations({ mutations });
  console.log("Pull successful on client");
};

const transactionHandler = async (transaction: Transaction) => {
  const { tableDescriptors } = await getCatalogSnapshot({
    logicalTimestamp: transaction.logicalTimestamp - 1,
  });

  const schema = new Schema(tableDescriptors);

  const pushRows: Map<number, Scalar> = schema.getAffectedRows(
    getEnv("DATALAND_CLIENTS_TABLE_NAME"),
    "MBO push",
    transaction
  );
  const pullRows: Map<number, Scalar> = schema.getAffectedRows(
    getEnv("DATALAND_CLIENTS_TABLE_NAME"),
    "MBO pull",
    transaction
  );

  if (pushRows.size !== 0) {
    pushRow(pushRows, transaction.logicalTimestamp, schema);
  }

  if (pullRows.size !== 0) {
    pullRow(pullRows, transaction.logicalTimestamp, schema);
  }
};

console.log("registering transaction handler");
registerTransactionHandler(transactionHandler);

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
import { clientPostT } from "./client";
import { DATALAND_CLIENTS_TABLE_NAME } from "./constants";
import { parseClients } from "./importCron";
import { postUpdateClient } from "./writeBack";

const transactionHandler = async (transaction: Transaction) => {
  const t = await querySqlSnapshot({
    logicalTimestamp: transaction.logicalTimestamp - 1,
    sqlQuery: `select * from "test"`,
  });
  const r = unpackRows(t);
  console.log(r);

  const { tableDescriptors } = await getCatalogSnapshot({
    logicalTimestamp: transaction.logicalTimestamp - 1,
  });

  const schema = new Schema(tableDescriptors);

  const mutation = schema.makeInsertRows(DATALAND_CLIENTS_TABLE_NAME, 2, {
    number1: 1,
    number2: 2,
  });
  await runMutations({ mutations: [mutation] });

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

    const cli: any = {};
    for (const columnName in mbRow) {
      const value = mbRow[columnName];
      const parsedValue = (() => {
        // NOTE(gab): our backend returns NaN for empty number fields
        if (typeof value === "number" && isNaN(value)) {
          return null;
        }

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

        let parentProperty = cli[parentPropertyKey];
        if (parentProperty == null) {
          parentProperty = {};
        }
        parentProperty[propertyKey] = parsedValue;

        cli[parentPropertyKey] = parentProperty;
      } else {
        cli[columnName] = parsedValue;
      }
    }

    const values: Record<string, Scalar> = {};

    console.log("CLII", cli);
    const postClient = clientPostT.safeParse(cli);
    if (postClient.success === true) {
      const resp = await postUpdateClient(postClient.data);

      const client = resp.client;
      if (client != null) {
        console.log("BEFORE PARSED CLIENT", client);
        const parsedClient = parseClients([client])[0]!;
        console.log("PARSED CLIENT", parsedClient);
        for (const clientKey in parsedClient) {
          const clientValue = parsedClient[clientKey];
          values[clientKey] = clientValue;
        }
      }

      values["MBO push status"] = resp.message;
      values["MBO pushed at"] = new Date().toISOString();
    } else {
      values["MBO push status"] = `Incorrect data types: ${JSON.stringify(
        postClient.error.issues
      )}`;
      values["MBO pushed at"] = new Date().toISOString();
    }

    console.log("sending", values);
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

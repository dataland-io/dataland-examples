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
import { DATALAND_CLIENTS_TABLE_NAME } from "./constants";
import { fetchClients, parseClients } from "./importCron";
import { postUpdateClient } from "./writeBack";

const transactionHandler = async (transaction: Transaction) => {
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
      ...mindBodyRow
    } = row;
    const key = _dataland_key as number;

    const resp = await postUpdateClient(mindBodyRow);
    const values: Record<string, Scalar> = {
      "MBO push status": resp.message,
      "MBO pushed at": new Date().toISOString(),
    };

    const client = resp.client;
    if (client != null) {
      const parsedClient = parseClients([client])[0]!;
      for (const clientKey in parsedClient) {
        const clientValue = parsedClient[clientKey];
        values[clientKey] = clientValue;
      }
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

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
} from "@dataland-io/dataland-sdk-worker";
import { DATALAND_CLIENTS_TABLE_NAME } from "./constants";
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
    const { _dataland_key, _dataland_ordinal, ...mindBodyRow } = row;
    const respText = await postUpdateClient(mindBodyRow);

    const values: Record<string, string> = {
      "MBO push status": respText,
    };
    if (!respText.startsWith("500")) {
      values["MBO pushed at"] = new Date().toISOString();
    }

    const key = row["_dataland_key"] as number;
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

import {
  getCatalogSnapshot,
  Mutation,
  KeyGenerator,
  OrdinalGenerator,
  querySqlSnapshot,
  registerTransactionHandler,
  runMutations,
  Schema,
  Transaction,
  unpackRows,
} from "@dataland-io/dataland-sdk-worker";

const handler = async (transaction: Transaction) => {
  const { tableDescriptors } = await getCatalogSnapshot({
    logicalTimestamp: transaction.logicalTimestamp,
  });

  const schema = new Schema(tableDescriptors);

  const affectedRows = schema.getAffectedRows(
    "Repro Trigger",
    "Trigger",
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

  const response = await querySqlSnapshot({
    logicalTimestamp: transaction.logicalTimestamp,
    sqlQuery: `select
      _dataland_key, Region
    from "Records from CSV"`,
  });

  console.log(
    "response.arrowRecordBatches.length: ",
    response.arrowRecordBatches.length
  );

  const rows = unpackRows(response);

  console.log("rows length: ", rows.length);

  const count_response = await querySqlSnapshot({
    logicalTimestamp: transaction.logicalTimestamp,
    sqlQuery: `select
      COUNT(*) as count
    from "Records from CSV"`,
  });

  const count_row = unpackRows(count_response);
  console.log("count_row: ", count_row);
};

registerTransactionHandler(handler);

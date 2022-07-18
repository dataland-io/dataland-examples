import {
  getCatalogSnapshot,
  Mutation,
  KeyGenerator,
  OrdinalGenerator,
  getArrowTable,
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

  const count_response = await querySqlSnapshot({
    logicalTimestamp: transaction.logicalTimestamp,
    sqlQuery: `select
      COUNT(*) as count
    from "Records from CSV"`,
  });

  const count_row = unpackRows(count_response);
  console.log("count(*) query shows actual table size ", count_row);

  const response = await querySqlSnapshot({
    logicalTimestamp: transaction.logicalTimestamp,
    sqlQuery: `select
      _dataland_key, Region
    from "Records from CSV"`,
  });

  const rows = unpackRows(response);
  console.log("but rows length shows 1024 rows max: ", rows.length);

  const rows_arrow_table = getArrowTable(response.arrowRecordBatches);
  console.log("rows_arrow_table.numRows too: ", rows_arrow_table.numRows);
};

registerTransactionHandler(handler);

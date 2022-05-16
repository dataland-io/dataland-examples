import {
  getCatalogSnapshot,
  Mutation,
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

  const response = await querySqlSnapshot({
    logicalTimestamp: transaction.logicalTimestamp,
    sqlQuery: "select _dataland_key, original, duplicate from mirror",
  });

  const rows = unpackRows(response);

  const schema = new Schema(tableDescriptors);

  const mutations: Mutation[] = [];
  for (const row of rows) {
    const key = Number(row["_dataland_key"]);
    const original = row["original"];
    const duplicate = row["duplicate"];
    if (original === duplicate) {
      continue;
    }
    const update = schema.makeUpdateRows("mirror", key, {
      duplicate: original,
    });
    mutations.push(update);
  }

  if (mutations.length === 0) {
    return;
  }

  await runMutations({ mutations });
};

registerTransactionHandler(handler);

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
  const schema = new Schema(tableDescriptors);

  const response = await querySqlSnapshot({
    logicalTimestamp: transaction.logicalTimestamp,
    sqlQuery: "select _dataland_key, name, greeting from greetings",
  });

  const rows = unpackRows(response);

  const mutations: Mutation[] = [];
  for (const row of rows) {
    const key = row["_dataland_key"] as number;
    const name = row["name"];
    const greeting = `Hello, ${name}!`;

    if (name === "" || greeting === row["greeting"]) {
      continue;
    }

    const update = schema.makeUpdateRows("greetings", key, {
      greeting,
    });

    mutations.push(update);
  }

  if (mutations.length === 0) {
    return;
  }

  await runMutations({ mutations });
};

registerTransactionHandler(handler);

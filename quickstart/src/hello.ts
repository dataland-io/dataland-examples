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

// This handler function runs after every database transaction
const handler = async (transaction: Transaction) => {
  // Get the schema
  const { tableDescriptors } = await getCatalogSnapshot({
    logicalTimestamp: transaction.logicalTimestamp,
  });
  const schema = new Schema(tableDescriptors);

  // Query the "greetings" table with arbitrary SQL
  const queryResponse = await querySqlSnapshot({
    logicalTimestamp: transaction.logicalTimestamp,
    sqlQuery: "select _dataland_key, name from greetings",
  });
  // Unpack the query result into plain JS objects
  const rows = unpackRows(queryResponse);

  // Iterate over the rows
  const mutations: Mutation[] = [];
  for (const row of rows) {
    const key = row["_dataland_key"] as number;

    // Construct the appropriate greeting message based on the name.
    const name = row["name"];
    const message = name != null && name !== "" ? `Hello, ${name}!` : null;

    // Create an "update row" mutation, using the row key to specify which row is to be updated,
    // and setting the greeting column to the message we constructed.
    const update = schema.makeUpdateRows("greetings", key, {
      greeting: message,
    });

    mutations.push(update);
  }

  // Perform the mutations in one atomic transaction
  await runMutations({ mutations });
};

// Register the handler function
registerTransactionHandler(handler);

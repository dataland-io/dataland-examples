import {
  getDbClient,
  getHistoryClient,
  MutationsBuilder,
  registerTransactionHandler,
  Transaction,
  unpackRows,
} from "@dataland-io/dataland-sdk";

// This handler function runs after every database transaction
const handler = async (transaction: Transaction) => {
  // Initialize Db and History clients
  const db = getDbClient();
  const history = getHistoryClient();

  // Query the "greetings" table with arbitrary SQL
  const queryResponse = await history.querySqlSnapshot({
    logicalTimestamp: transaction.logicalTimestamp,
    sqlQuery: "select _row_id, name from greetings",
  }).response;

  // Unpack the query result into plain JS objects
  const rows = unpackRows(queryResponse);

  // Iterate over the rows
  for (const row of rows) {
    const key = row._row_id as number;

    // Construct the appropriate greeting message based on the name.
    const name = row.name;
    const message = name != null && name !== "" ? `Hello, ${name}!` : null;

    // Create and run an "update row" mutation, using the row key to specify which row is to be updated,
    // and setting the greeting column to the message we constructed.
    await new MutationsBuilder()
      .updateRow("greetings", key, {
        greeting: message,
      })
      .run(db);
  }
};

// Register the handler function
registerTransactionHandler(handler);

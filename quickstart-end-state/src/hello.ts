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
    sqlQuery: "select _dataland_key, name, location from greetings",
  });
  // Unpack the query result into plain JS objects
  const rows = unpackRows(queryResponse);

  // Iterate over the rows
  const mutations: Mutation[] = [];
  for (const row of rows) {
    const key = row["_dataland_key"] as number;

    // Construct the appropriate greeting message based on the name.
    const name = row["name"];
    let message = name != null && name !== "" ? `Hello, ${name}!` : null;

    const location = row["location"];
    if (message != null && location != null && location !== "") {
      const weather = await lookupWeather(location as string);
      message = message + " " + weather;
    }

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

const lookupWeather = async (location: string): Promise<string | null> => {
  const normalizedLocation = location
    .split(/[^a-zA-Z]/)
    .join("")
    .toLowerCase();

  const response = await fetch(
    `https://weatherdbi.herokuapp.com/data/weather/${normalizedLocation}`
  );
  if (!response.ok) {
    return null;
  }
  const json = await response.json();

  const region = json.region;
  const temperature = json.currentConditions?.temp?.f;
  const description = json.currentConditions?.comment;

  if (region == null || temperature == null || description == null) {
    return null;
  }

  return `The weather in ${region} is ${temperature}°F and ${description.toLowerCase()}.`;
};

// Register the handler function
registerTransactionHandler(handler);

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
      const start = performance.now();
      const weather = await lookupWeather(location as string);
      const end = performance.now();
      console.log(`weather lookup took ${end - start}ms`);
      if (weather != null) {
        message = message + " " + weather;
      }
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
    const responseText = await response.text();
    console.error(
      "weather api call failed",
      location,
      response.status,
      responseText
    );
    return null;
  }
  const json = await response.json();

  const region = json.region;
  const temperature = json.currentConditions?.temp?.f;
  const description = json.currentConditions?.comment;

  if (region == null || temperature == null || description == null) {
    console.error("weather api returned unexpected response", json);
    return null;
  }

  console.log("weather api call succeeded", json);

  return `The weather in ${region} is ${temperature}°F and ${description.toLowerCase()}.`;
};

// Register the handler function
registerTransactionHandler(handler);

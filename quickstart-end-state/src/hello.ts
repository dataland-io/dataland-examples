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
    sqlQuery: "select _row_id, location, name from greetings",
  }).response;

  // Unpack the query result into plain JS objects
  const rows = unpackRows(queryResponse);

  // Iterate over the rows
  for (const row of rows) {
    const key = row._row_id as number;

    // Construct the appropriate greeting message based on the name.
    const name = row.name;
    let message = name != null && name !== "" ? `Hello, ${name}!` : null;

    const location = row.location;
    if (message != null && location != null && location !== "") {
      const start = performance.now();
      const weather = await lookupWeather(location as string);
      const end = performance.now();
      console.log(`weather lookup took ${end - start}ms`);
      if (weather != null) {
        message = message + " " + weather;
      }
    }

    // Create and run an "update row" mutation, using the row key to specify which row is to be updated,
    // and setting the greeting column to the message we constructed.
    await new MutationsBuilder()
      .updateRow("greetings", key, {
        greeting: message,
      })
      .run(db);
  }
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
    console.error("weather api returned unexpected response", location, json);
    return null;
  }

  console.log("weather api call succeeded", location, json);

  return `The weather in ${region} is ${temperature}°F and ${description.toLowerCase()}.`;
};

// Register the handler function
registerTransactionHandler(handler);

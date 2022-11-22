import {
  getDbClient,
  getHistoryClient,
  MutationsBuilder,
  registerTransactionHandler,
  Transaction,
  unpackRows,
  isString,
} from "@dataland-io/dataland-sdk";

// This handler function runs after every database transaction
const handler = async (transaction: Transaction) => {
  // Initialize Db and History clients
  const db = getDbClient();
  const history = getHistoryClient();

  const affected_row_ids = [];

  for (const mutation of transaction.mutations) {
    if (mutation.kind.oneofKind == "updateRows") {
      if (
        // TODO: Replace with the columnName of your own button column and tablename
        mutation.kind.updateRows.columnNames.includes("get_weather") &&
        mutation.kind.updateRows.tableName === "weather_table"
      ) {
        for (const row of mutation.kind.updateRows.rows) {
          affected_row_ids.push(row.rowId);
        }
      } else {
        return;
      }
    } else {
      return;
    }
  }

  const affected_row_ids_key_list = affected_row_ids.join(",");

  // The following code fetches the rows where the button column was clicked.
  // TODO: Replace the tableName with your own, and replace the columns specified here with
  // the columns you'll need to use values from to make the API call
  const response = await history.querySqlMirror({
    sqlQuery: `select _row_id, location from "weather_table" where _row_id in (${affected_row_ids_key_list})`,
  }).response;

  const rows = unpackRows(response);

  if (rows == null) {
    return;
  }

  // Iterate over the rows
  for (const row of rows) {
    const key = row._row_id as number;

    // TODO: Grab the value from the columns you need to make the API call
    const location = row.location;

    if (!isString(location)) {
      console.error("location is not a string", location);
      continue;
    }
    // TODO: Call your own API
    const weather = await lookupWeather(location);

    // Create and run an "update row" mutation, using the row key to specify which row is to be updated,
    // and setting the "weather_result" column to the message we constructed.
    // TODO: Change the table name + column names below, and pass in the result of your API call
    await new MutationsBuilder()
      .updateRow("weather_table", key, {
        weather_result: weather,
        checked_at: new Date().toISOString(),
      })
      .run(db);
  }
};

// TOOD: Define your own API call
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

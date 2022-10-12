// This is a template.
// To use it, search for `TODO` and follow the steps for your use case.

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

// TODO: First, replace the following URL with a URL that has the plain-text CSV content of your file.
// One way to do this is using Github Gists.
// See this video for instructions: https://www.loom.com/share/fb19460ff28344558d8986a362b1293c).

const CSV_URL =
  "https://gist.githubusercontent.com/arthurwuhoo/2373fdd22298d7af48146bb37ba04983/raw/9eb8619c19f964d7f0c1cfb6c745c1cb49cffd5d/every-dataland-data-type.csv";

const fetchCSVfromCurl = async (url: string) => {
  const csv_url = url;
  const response = await fetch(csv_url);
  const data = await response.text();
  return data;
};

function csvToRowArray(csv: string) {
  var lines = csv.split("\n");
  var result = [];

  // This assumes that the CSV has column names in its first line, so it skips it (since i = 1 to start)
  for (var i = 1; i < lines.length; i++) {
    // TODO: Change the delimiter here if it's not a comma.
    let currentline = lines[i].split(",");

    // TODO: Change the keys of this object to your column names here.
    // For example below, the first column will be stored with object key "region", and there are 14 columns total.
    let row = {
      Select: currentline[0],
      Button: currentline[1],
      Checkbox: currentline[2],
      Text: currentline[3],
      Number: currentline[4],
      URL: currentline[5],
      "Raw: String": currentline[6],
      "Raw: Boolean": currentline[7],
      "Raw: int32": currentline[8],
      "Raw: int64": currentline[9],
      "Raw: float32": currentline[10],
      "Raw: float64": currentline[11],
    };
    result.push(row);
  }
  return result;
}

// --------------------------------------------------
const handler = async (transaction: Transaction) => {
  const { tableDescriptors } = await getCatalogSnapshot({
    logicalTimestamp: transaction.logicalTimestamp,
  });

  const schema = new Schema(tableDescriptors);

  // This function will only get triggered if a user presses a button in a button column named "Trigger",
  // that's part of a table called "import_trigger".

  // TODO: If you're using a different name for the trigger table or its column, change it here.
  const affectedRows = schema.getAffectedRows(
    "import_trigger",
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
      _dataland_key
    from "import_trigger"
    where _dataland_key in ${keyList}`,
  });

  const trigger_rows = unpackRows(response);

  const keyGenerator = new KeyGenerator();
  const ordinalGenerator = new OrdinalGenerator();

  const mutations: Mutation[] = [];
  for (const trigger_row of trigger_rows) {
    const key = Number(trigger_row["_dataland_key"]);
    console.log("key: ", key);

    const update = schema.makeUpdateRows("import_trigger", key, {
      "Last pressed": new Date().toISOString(),
    });

    if (update == null) {
      console.log("No update found");
      continue;
    }
    mutations.push(update);
    console.log("mutations: ", mutations);
  }

  const records = await fetchCSVfromCurl(CSV_URL);
  const result = csvToRowArray(records);

  let mutations_batch: Mutation[] = [];
  let csv_row_count = result.length;
  console.log("Total rows in CSV: ", csv_row_count);
  let batch_counter = 0;
  let total_counter = 0;

  while (total_counter < csv_row_count) {
    while (batch_counter < 500) {
      const record = result[total_counter];

      if (record == null) {
        console.log("No more rows in CSV");
        break;
      }

      // TODO: Update the variable definitions below based on how you named the row object keys above.

      // Select: currentline[0],
      const select = record.Select;
      // Button: currentline[1],
      const button = record.Button;
      // Checkbox: currentline[2],
      const checkbox = record.Checkbox;
      // Text: currentline[3],
      const text = record.Text;
      // Number: currentline[4],
      const number = record.Number;
      // URL: currentline[5],
      const url = record.URL;
      // "Raw: String": currentline[6],
      const raw_string = record["Raw: String"];
      // "Raw: Boolean": currentline[7],
      const raw_boolean = record["Raw: Boolean"];
      // "Raw: int32": currentline[8],
      const raw_int32 = record["Raw: int32"];
      // "Raw: int64": currentline[9],
      const raw_int64 = record["Raw: int64"];
      // "Raw: float32": currentline[10],
      const raw_float32 = record["Raw: float32"];
      // "Raw: float64": currentline[11],
      const raw_float64 = record["Raw: float64"];

      const id = await keyGenerator.nextKey();
      const ordinal = await ordinalGenerator.nextOrdinal();

      // TODO: Update the table name (currently "records_from_csv") to the actual name of the imported rows table.
      const insert = schema.makeInsertRows("records_from_csv", id, {
        _dataland_ordinal: ordinal,

        // TODO: Update the keys below to correspond to the column names in the imported rows table.
        // Remember to align spec.yaml by declaring the same columnNames in the table schema.
        // The format is {columnName}: {row_value}
        Select: select,
        Button: button,
        Checkbox: checkbox,
        Text: text,
        Number: number,
        URL: url,
        "Raw: String": raw_string,
        "Raw: Boolean": raw_boolean,
        "Raw: int32": raw_int32,
        "Raw: int64": raw_int64,
        "Raw: float32": raw_float32,
        "Raw: float64": raw_float64,
      });
      if (insert == null) {
        continue;
      }
      mutations_batch.push(insert);
      total_counter++;
      batch_counter++;
    }
    await runMutations({ mutations: mutations_batch });
    console.log("Processed row count: ", total_counter);
    batch_counter = 0;
    mutations_batch = [];
  }
  await runMutations({ mutations });
};

registerTransactionHandler(handler);

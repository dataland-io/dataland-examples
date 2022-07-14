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

// part 1:
const CSV_URL =
  "https://gist.githubusercontent.com/arthurwuhoo/c8f7d8bd4f81a852889139e7d7cdf2eb/raw/ac605ff281fbb96f3465af64942357ee8e7652d5/partsupp-pt1-1.csv";

// part 2:
// const CSV_URL =
// "https://gist.githubusercontent.com/arthurwuhoo/704e2ea95817b8ca8e2675bc76dbdbd7/raw/cc967742db33b57e38805007b5cb4274a11f0260/partsupp-pt1-2.csv";

// part 3:
// const CSV_URL =
// "https://gist.githubusercontent.com/arthurwuhoo/b96509c296826552ee807a3edd480651/raw/34a2637cc613a3d0b75ed842793c0fbbe34bc540/partsupp-pt2.csv";

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
    let currentline = lines[i].split("|");

    // TODO: Change the keys of this object to your column names here.
    // For example below, the first column will be stored with object key "region", and there are 14 columns total.
    let row = {
      ps_partkey: currentline[0],
      ps_suppkey: currentline[1],
      ps_availqty: currentline[2],
      ps_supplycost: currentline[3],
      ps_comment: currentline[4],
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
  // that's part of a table called "Import Trigger".

  // TODO: If you're using a different name for the trigger table or its column, change it here.
  const affectedRows = schema.getAffectedRows(
    "Import Trigger",
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
    from "Import Trigger"
    where _dataland_key in ${keyList}`,
  });

  const trigger_rows = unpackRows(response);

  const keyGenerator = new KeyGenerator();
  const ordinalGenerator = new OrdinalGenerator();

  const mutations: Mutation[] = [];
  for (const trigger_row of trigger_rows) {
    const key = Number(trigger_row["_dataland_key"]);
    console.log("key: ", key);

    const update = schema.makeUpdateRows("Import Trigger", key, {
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
      const ps_partkey = record.ps_partkey;
      const ps_suppkey = record.ps_suppkey;
      const ps_availqty = record.ps_availqty;
      const ps_supplycost = record.ps_supplycost;
      const ps_comment = record.ps_comment;

      const id = await keyGenerator.nextKey();
      const ordinal = await ordinalGenerator.nextOrdinal();

      // TODO: Update the table name (currently "Records from CSV") to the actual name of the imported rows table.
      const insert = schema.makeInsertRows("Records from CSV", id, {
        _dataland_ordinal: ordinal,

        // TODO: Update the keys below to correspond to the column names in the imported rows table.
        // Remember to align spec.yaml by declaring the same columnNames in the table schema.
        // The format is {columnName}: {row_value}
        ps_partkey: ps_partkey,
        ps_suppkey: ps_suppkey,
        ps_availqty: ps_availqty,
        ps_supplycost: ps_supplycost,
        ps_comment: ps_comment,
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

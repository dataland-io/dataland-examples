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
  "https://gist.githubusercontent.com/arthurwuhoo/5a32ceb175778f86a8b0353fadb8c50d/raw/d9735007478a587dfbf4c75db3f07a75b2e5a786/sales";

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
      region: currentline[0],
      country: currentline[1],
      item_type: currentline[2],
      sales_channel: currentline[3],
      order_priority: currentline[4],
      order_date: currentline[5],
      order_id: currentline[6],
      ship_date: currentline[7],
      units_sold: currentline[8],
      unit_price: currentline[9],
      unit_cost: currentline[10],
      total_revenue: currentline[11],
      total_cost: currentline[12],
      total_profit: currentline[13],
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
      const region = record.region;
      const country = record.country;
      const item_type = record.item_type;
      const sales_channel = record.sales_channel;
      const order_priority = record.order_priority;
      const order_date = record.order_date;
      const order_id = record.order_id;
      const ship_date = record.ship_date;
      const units_sold = record.units_sold;
      const unit_price = record.unit_price;
      const unit_cost = record.unit_cost;
      const total_revenue = record.total_revenue;
      const total_cost = record.total_cost;
      const total_profit = record.total_profit;

      const id = await keyGenerator.nextKey();
      const ordinal = await ordinalGenerator.nextOrdinal();

      // TODO: Update the table name (currently "Records from CSV") to the actual name of the imported rows table.
      const insert = schema.makeInsertRows("Records from CSV", id, {
        _dataland_ordinal: ordinal,

        // TODO: Update the keys below to correspond to the column names in the imported rows table.
        // Remember to align spec.yaml by declaring the same columnNames in the table schema.
        // The format is {columnName}: {row_value}
        Region: region,
        Country: country,
        "Item Type": item_type,
        "Sales Channel": sales_channel,
        "Order Priority": order_priority,
        "Order Date": order_date,
        "Order ID": order_id,
        "Ship Date": ship_date,
        "Units Sold": units_sold,
        "Unit Price": unit_price,
        "Unit Cost": unit_cost,
        "Total Revenue": total_revenue,
        "Total Cost": total_cost,
        "Total Profit": total_profit,
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

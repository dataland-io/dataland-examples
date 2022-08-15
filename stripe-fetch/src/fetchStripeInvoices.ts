import {
  getEnv,
  syncTables,
  SyncTable,
  registerCronHandler,
} from "@dataland-io/dataland-sdk-worker";

import { tableFromJSON, tableToIPC } from "@apache-arrow/es2015-esm";

import { isNumber } from "lodash-es";

const stripe_key = getEnv("STRIPE_API_KEY");

const fetchStripeinvoices = async () => {
  var headers = new Headers();
  headers.append("Content-Type", "application/x-www-form-urlencoded");
  headers.append("Authorization", `Bearer ${stripe_key}`);

  let total_counter = 0;
  const full_results = [];

  let url = "https://api.stripe.com//v1/invoices?limit=100";
  let has_more = true;

  do {
    const stripe_response = await fetch(url, {
      method: "GET",
      headers: headers,
      redirect: "follow",
    });
    const data = await stripe_response.json();
    has_more = data.has_more;
    url = url + "&starting_after=" + data.data[data.data.length - 1].id;
    const results = data.data;

    if (results) {
      for (const result of results) {
        full_results.push(result);
        total_counter++;
        console.log("id: ", result.id, " – total_counter: ", total_counter);
      }
    }
  } while (has_more);

  return full_results;
};

const handler = async () => {
  // fetch Stripe invoices from Stripe
  const records = await fetchStripeinvoices();
  console.log("xx records done", records.length);
  const table = tableFromJSON(records);
  console.log("xx table done", table);
  const batch = tableToIPC(table);
  console.log("xx batch done", batch);

  const syncTable: SyncTable = {
    tableName: "stripe-refunds",
    arrowRecordBatches: [batch],
    identityColumnNames: ["id"],
  };
  console.log("xx syncTable done", syncTable);

  await syncTables({ syncTables: [syncTable] });
  console.log("Sync done");
};

registerCronHandler(handler);

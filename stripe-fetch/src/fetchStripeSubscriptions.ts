import {
  getEnv,
  syncTables,
  SyncTable,
  registerCronHandler,
} from "@dataland-io/dataland-sdk-worker";

import { tableFromJSON, tableToIPC } from "@apache-arrow/es2015-esm";

const stripe_key = getEnv("STRIPE_API_KEY");

const fetchStripeSubscriptions = async () => {
  var headers = new Headers();
  headers.append("Content-Type", "application/x-www-form-urlencoded");
  headers.append("Authorization", `Bearer ${stripe_key}`);

  let total_counter = 0;
  const full_results = [];

  let url = "https://api.stripe.com//v1/subscriptions?limit=100";
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
        console.log(
          "Subscription id: ",
          result.id,
          " – total_counter: ",
          total_counter
        );
      }
    }
  } while (has_more);

  return full_results;
};

const fetchStripeSubscriptionItems = async (subscription_id: string) => {
  var headers = new Headers();

  headers.append("Content-Type", "application/x-www-form-urlencoded");
  headers.append("Authorization", `Bearer ${stripe_key}`);

  let total_counter = 0;
  const full_results = [];

  let url = `https://api.stripe.com//v1/subscription_items?subscription=${subscription_id}&limit=100`;
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
        console.log(
          "Subscription item id: ",
          result.id,
          " – total_counter: ",
          total_counter
        );
      }
    }
  } while (has_more);

  return full_results;
};

const handler = async () => {
  const subscriptions_records = await fetchStripeSubscriptions();
  console.log("Subscriptions records done", subscriptions_records.length);
  const subscriptions_table = tableFromJSON(subscriptions_records);
  console.log("Subscriptions table done", subscriptions_table);
  const subscriptions_batch = tableToIPC(subscriptions_table);
  console.log("Subscriptions batch done", subscriptions_batch);

  const subscriptions_syncTable: SyncTable = {
    tableName: "stripe-subscriptions",
    arrowRecordBatches: [subscriptions_batch],
    identityColumnNames: ["id"],
  };
  console.log("Subscriptions syncTable done", subscriptions_syncTable);

  await syncTables({ syncTables: [subscriptions_syncTable] });
  console.log("Sync of subscriptions done");

  const subscription_items_records = [];

  for (const record of subscriptions_records) {
    const subscription_id = record.id;
    const subscription_items = await fetchStripeSubscriptionItems(
      subscription_id
    );
    for (const subscription_item of subscription_items) {
      subscription_items_records.push(subscription_item);
    }
  }

  console.log(
    "Subscription items records done",
    subscription_items_records.length
  );
  const subscription_items_table = tableFromJSON(subscription_items_records);
  console.log("Subscription items table done", subscription_items_table);
  const subscription_items_batch = tableToIPC(subscription_items_table);
  console.log("Subscription items batch done", subscription_items_batch);

  const subscription_items_syncTable: SyncTable = {
    tableName: "stripe-subscription-items",
    arrowRecordBatches: [subscription_items_batch],
    identityColumnNames: ["id"],
  };
  console.log(
    "Subscription items syncTable done",
    subscription_items_syncTable
  );

  await syncTables({ syncTables: [subscription_items_syncTable] });
  console.log("Sync of subscription items done");
};

registerCronHandler(handler);

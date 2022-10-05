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
        const stripeSubscription = {
          id: result.id,
          created: result.created,
          cancel_at_period_end: result.cancel_at_period_end,
          current_period_end: result.current_period_end,
          current_period_start: result.current_period_start,
          customer: result.customer,
          default_payment_method: result.default_payment_method,
          description: result.description,
          items: JSON.stringify(result.items),
          latest_invoice: result.latest_invoice,
          metadata: JSON.stringify(result.metadata),
          status: result.status,
        };
        full_results.push(stripeSubscription);
      }
    }
  } while (has_more);

  return full_results;
};

const fetchStripeSubscriptionItems = async (subscription_id: string) => {
  var headers = new Headers();

  headers.append("Content-Type", "application/x-www-form-urlencoded");
  headers.append("Authorization", `Bearer ${stripe_key}`);

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
        const stripeSubscriptionItem = {
          id: result.id,
          metadata: JSON.stringify(result.metadata),
          price_obj: JSON.stringify(result.price),
          quantity: result.quantity,
          subscription: result.subscription,
          price_id: result.price.id,
          price_currency: result.price.currency,
          price_unit_amount: result.price.unit_amount,
          product_id: result.price.product,
        };
        full_results.push(stripeSubscriptionItem);
      }
    }
  } while (has_more);

  return full_results;
};

const handler = async () => {
  console.log("fetching Stripe subscriptions...");
  const subscriptions_records = await fetchStripeSubscriptions();
  console.log(
    "fetched ",
    subscriptions_records.length,
    " Stripe subscriptions"
  );
  const subscriptions_table = tableFromJSON(subscriptions_records);
  const subscriptions_batch = tableToIPC(subscriptions_table);

  const subscriptions_syncTable: SyncTable = {
    tableName: "stripe_subscriptions",
    arrowRecordBatches: [subscriptions_batch],
    identityColumnNames: ["id"],
    keepExtraColumns: true,
  };

  await syncTables({ syncTables: [subscriptions_syncTable] });
  console.log("synced Stripe subscriptions to Dataland");

  const subscription_items_records = [];

  console.log("fetching Stripe subscription items...");
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
    "fetched ",
    subscription_items_records.length,
    " Stripe subscription items"
  );

  const subscription_items_table = tableFromJSON(subscription_items_records);
  const subscription_items_batch = tableToIPC(subscription_items_table);

  const subscription_items_syncTable: SyncTable = {
    tableName: "stripe_subscription_items",
    arrowRecordBatches: [subscription_items_batch],
    identityColumnNames: ["id"],
    keepExtraColumns: true,
  };

  await syncTables({ syncTables: [subscription_items_syncTable] });
  console.log("synced Stripe subscription items to Dataland");
};

registerCronHandler(handler);

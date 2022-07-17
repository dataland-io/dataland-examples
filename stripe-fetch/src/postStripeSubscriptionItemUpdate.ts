import {
  getCatalogSnapshot,
  getEnv,
  Mutation,
  querySqlSnapshot,
  KeyGenerator,
  OrdinalGenerator,
  registerTransactionHandler,
  runMutations,
  Schema,
  Transaction,
  unpackRows,
} from "@dataland-io/dataland-sdk-worker";

import { isString, isNumber } from "lodash-es";

const stripe_key = getEnv("STRIPE_API_KEY");

const incrementStripeSubscriptionItemQuantity = async (
  subscription_item_id: string
) => {
  var headers = new Headers();
  headers.append("Content-Type", "application/x-www-form-urlencoded");
  headers.append("Authorization", `Bearer ${stripe_key}`);

  const subscription_item_url =
    "https://api.stripe.com/v1/subscription_items/" + subscription_item_id;

  const get_options = {
    method: "GET",
    headers: headers,
  };

  const get_response = await fetch(subscription_item_url, get_options);
  const subscriptionItem = await get_response.json();
  const quantity = subscriptionItem.quantity;
  console.log("xx - get response: " + JSON.stringify(subscriptionItem));

  const new_quantity = quantity + 1;
  const post_options = {
    method: "POST",
    headers: headers,
    body: JSON.stringify({ quantity: new_quantity }),
  };

  const post_response = await fetch(subscription_item_url, post_options);
  const post_response_json = await post_response.json();
  console.log("xx - post response: " + JSON.stringify(post_response_json));

  return post_response_json;
};

// TODO: function is defined, now need to call it from a button

const handler = async (transaction: Transaction) => {
  const { tableDescriptors } = await getCatalogSnapshot({
    logicalTimestamp: transaction.logicalTimestamp,
  });

  const schema = new Schema(tableDescriptors);

  const affectedRows = schema.getAffectedRows(
    "stripe-subscriptions-trigger",
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

  const trigger_response = await querySqlSnapshot({
    logicalTimestamp: transaction.logicalTimestamp,
    sqlQuery: `select
      _dataland_key
    from "stripe-subscriptions-trigger"
    where _dataland_key in ${keyList}`,
  });

  const trigger_rows = unpackRows(trigger_response);

  const keyGenerator = new KeyGenerator();
  const ordinalGenerator = new OrdinalGenerator();

  // fetch Stripe subscriptions from Stripe
  const stripeSubscriptions = await fetchStripeSubscriptions();

  if (stripeSubscriptions == null) {
    return;
  }

  // fetch existing Stripe subscriptions
  const existing_stripe_data = await querySqlSnapshot({
    logicalTimestamp: transaction.logicalTimestamp,
    sqlQuery: `select
      _dataland_key, id
    from "stripe-subscriptions"`,
  });

  const existing_stripe_rows = unpackRows(existing_stripe_data);

  const existing_stripe_ids = [];
  const existing_stripe_keys = [];

  for (const existing_stripe_row of existing_stripe_rows) {
    existing_stripe_keys.push(existing_stripe_row._dataland_key);
    existing_stripe_ids.push(existing_stripe_row.id);
  }

  let mutations_batch: Mutation[] = [];
  let batch_counter = 0;
  let batch_size = 100; // push 100 updates/inserts at a time
  let total_counter = 0;

  for (const stripeSubscription of stripeSubscriptions) {
    // Generate a new _dataland_key and _dataland_ordinal value
    const id = await keyGenerator.nextKey();
    const ordinal = await ordinalGenerator.nextOrdinal();

    const stripe_subscription_id = String(stripeSubscription.id);

    if (stripe_subscription_id == null) {
      continue;
    }

    // check if the Stripe subscription already exists
    if (existing_stripe_ids.includes(stripe_subscription_id)) {
      const position = existing_stripe_ids.indexOf(stripe_subscription_id);
      const existing_key = existing_stripe_keys[position];

      if (!isNumber(existing_key)) {
        continue;
      }

      const update = schema.makeUpdateRows(
        "stripe-subscriptions",
        existing_key,
        {
          id: stripeSubscription.id,
          created: stripeSubscription.created,
          cancel_at_period_end: stripeSubscription.cancel_at_period_end,
          current_period_end: stripeSubscription.current_period_end,
          current_period_start: stripeSubscription.current_period_start,
          customer: stripeSubscription.customer,
          default_payment_method: stripeSubscription.default_payment_method,
          description: stripeSubscription.description,
          items: JSON.stringify(stripeSubscription.items),
          latest_invoice: stripeSubscription.latest_invoice,
          metadata: JSON.stringify(stripeSubscription.metadata),
          status: stripeSubscription.status,
        }
      );

      if (update == null) {
        continue;
      }
      mutations_batch.push(update);

      batch_counter++;
      total_counter++;
    } else {
      const insert = schema.makeInsertRows("stripe-subscriptions", id, {
        _dataland_ordinal: ordinal,
        id: stripeSubscription.id,
        created: stripeSubscription.created,
        cancel_at_period_end: stripeSubscription.cancel_at_period_end,
        current_period_end: stripeSubscription.current_period_end,
        current_period_start: stripeSubscription.current_period_start,
        customer: stripeSubscription.customer,
        default_payment_method: stripeSubscription.default_payment_method,
        description: stripeSubscription.description,
        items: JSON.stringify(stripeSubscription.items),
        latest_invoice: stripeSubscription.latest_invoice,
        metadata: JSON.stringify(stripeSubscription.metadata),
        status: stripeSubscription.status,
      });

      if (insert == null) {
        continue;
      }
      mutations_batch.push(insert);

      batch_counter++;
      total_counter++;
    }

    if (batch_counter >= batch_size) {
      await runMutations({ mutations: mutations_batch });
      mutations_batch = [];
      batch_counter = 0;
      console.log("total processed: ", total_counter);
    } else if (total_counter + batch_size > stripeSubscriptions.length) {
      await runMutations({ mutations: mutations_batch });
      mutations_batch = [];
      batch_counter = 0;
      console.log("total processed: ", total_counter);
    }
  }
};

registerTransactionHandler(handler);

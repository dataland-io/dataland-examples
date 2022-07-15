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

const fetchStripesubscriptions = async () => {
  var myHeaders = new Headers();
  myHeaders.append("Content-Type", "application/x-www-form-urlencoded");
  myHeaders.append("Authorization", `Bearer ${stripe_key}`);

  let total_counter = 0;
  const full_results = [];

  let url = "https://api.stripe.com//v1/subscriptions?limit=100";
  let has_more = true;

  do {
    const stripe_response = await fetch(url, {
      method: "GET",
      headers: myHeaders,
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
  const stripesubscriptions = await fetchStripesubscriptions();

  if (stripesubscriptions == null) {
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
  let batch_size = 100; // push 100 at a time
  let total_counter = 0;

  for (const stripeSubscription of stripesubscriptions) {
    // Generate an ID
    const id = await keyGenerator.nextKey();
    const ordinal = await ordinalGenerator.nextOrdinal();

    // grab the object_id for each Shippo Shipment
    const stripe_subscription_id = String(stripeSubscription.id);

    if (stripe_subscription_id == null) {
      continue;
    }

    // check if the Stripe subscription already exists
    if (existing_stripe_ids.includes(stripe_subscription_id)) {
      // get the _dataland_key of the

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
      console.log("total_counter: ", total_counter);
    } else if (total_counter + batch_size > stripesubscriptions.length) {
      await runMutations({ mutations: mutations_batch });
      mutations_batch = [];
      batch_counter = 0;
      console.log("total_counter: ", total_counter);
    }
  }
};

registerTransactionHandler(handler);

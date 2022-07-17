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

  const keyGeneratorSubscriptions = new KeyGenerator();
  const ordinalGeneratorSubscriptions = new OrdinalGenerator();

  const keyGeneratorSubscriptionItems = new KeyGenerator();
  const ordinalGeneratorSubscriptionItems = new OrdinalGenerator();

  // fetch Stripe subscriptions from Stripe
  const stripeSubscriptions = await fetchStripeSubscriptions();

  if (stripeSubscriptions == null) {
    return;
  }

  // fetch existing Stripe subscriptions
  const existing_stripe_subscriptions = await querySqlSnapshot({
    logicalTimestamp: transaction.logicalTimestamp,
    sqlQuery: `select
      _dataland_key, id
    from "stripe-subscriptions"`,
  });

  const existing_stripe_subscriptions_rows = unpackRows(
    existing_stripe_subscriptions
  );

  const existing_stripe_subscriptions_ids = [];
  const existing_stripe_subscriptions_keys = [];

  for (const existing_stripe_subscriptions_row of existing_stripe_subscriptions_rows) {
    existing_stripe_subscriptions_keys.push(
      existing_stripe_subscriptions_row._dataland_key
    );
    existing_stripe_subscriptions_ids.push(
      existing_stripe_subscriptions_row.id
    );
  }

  // fetch existing Stripe subscriptions items
  const existing_stripe_subscriptions_item = await querySqlSnapshot({
    logicalTimestamp: transaction.logicalTimestamp,
    sqlQuery: `select
        _dataland_key, id
      from "stripe-subscription-items"`,
  });

  const existing_stripe_subscriptions_item_rows = unpackRows(
    existing_stripe_subscriptions
  );

  const existing_stripe_subscriptions_item_ids = [];
  const existing_stripe_subscriptions_item_keys = [];

  for (const existing_stripe_subscriptions_item_row of existing_stripe_subscriptions_item_rows) {
    existing_stripe_subscriptions_item_keys.push(
      existing_stripe_subscriptions_item_row._dataland_key
    );
    existing_stripe_subscriptions_item_ids.push(
      existing_stripe_subscriptions_item_row.id
    );
  }

  let mutations_batch: Mutation[] = [];
  let batch_counter = 0;
  let batch_size = 100; // push 100 updates/inserts at a time
  let total_counter = 0;

  for (const stripeSubscription of stripeSubscriptions) {
    // Generate a new _dataland_key and _dataland_ordinal value
    const id = await keyGeneratorSubscriptions.nextKey();
    const ordinal = await ordinalGeneratorSubscriptions.nextOrdinal();

    const stripe_subscription_id = String(stripeSubscription.id);

    if (stripe_subscription_id == null) {
      continue;
    }

    // check if the Stripe subscription already exists
    if (existing_stripe_subscriptions_ids.includes(stripe_subscription_id)) {
      const position = existing_stripe_subscriptions_ids.indexOf(
        stripe_subscription_id
      );
      const existing_key = existing_stripe_subscriptions_keys[position];

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

    // fetch and loop through each subscription item
    const stripeSubscriptionItems = await fetchStripeSubscriptionItems(
      stripe_subscription_id
    );

    for (const stripeSubscriptionItem of stripeSubscriptionItems) {
      const stripe_subscription_item_id = String(stripeSubscriptionItem.id);
      const generated_subscription_item_key =
        await keyGeneratorSubscriptionItems.nextKey();
      const generated_subscription_item_ordinal =
        await ordinalGeneratorSubscriptionItems.nextOrdinal();

      if (stripe_subscription_item_id == null) {
        continue;
      }

      // check if id already exists
      if (
        existing_stripe_subscriptions_item_ids.includes(
          stripe_subscription_item_id
        )
      ) {
        const position = existing_stripe_subscriptions_item_ids.indexOf(
          stripe_subscription_item_id
        );
        const existing_item_key =
          existing_stripe_subscriptions_item_keys[position];

        if (!isNumber(existing_item_key)) {
          continue;
        }

        const update = schema.makeUpdateRows(
          "stripe-subscription-items",
          existing_item_key,
          {
            id: stripeSubscriptionItem.id,
            metadata: JSON.stringify(stripeSubscriptionItem.metadata),
            price: JSON.stringify(stripeSubscriptionItem.price),
            quantity: stripeSubscriptionItem.quantity,
            subscription: stripeSubscriptionItem.subscription,
            customer: stripeSubscription.customer,
          }
        );

        if (update == null) {
          continue;
        }
        mutations_batch.push(update);
      } else {
        const insert = schema.makeInsertRows(
          "stripe-subscription-items",
          generated_subscription_item_key,
          {
            _dataland_ordinal: generated_subscription_item_ordinal,
            id: stripeSubscriptionItem.id,
            metadata: JSON.stringify(stripeSubscriptionItem.metadata),
            price: JSON.stringify(stripeSubscriptionItem.price),
            quantity: stripeSubscriptionItem.quantity,
            subscription: stripeSubscriptionItem.subscription,
            customer: stripeSubscription.customer,
          }
        );

        if (insert == null) {
          continue;
        }
        mutations_batch.push(insert);
      }
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

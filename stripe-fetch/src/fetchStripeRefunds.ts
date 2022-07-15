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

const fetchStriperefunds = async () => {
  var myHeaders = new Headers();
  myHeaders.append("Content-Type", "application/x-www-form-urlencoded");
  myHeaders.append("Authorization", `Bearer ${stripe_key}`);

  let total_counter = 0;
  const full_results = [];

  let url = "https://api.stripe.com//v1/refunds?limit=100";
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
    "stripe-refunds-trigger",
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
    from "stripe-refunds-trigger"
    where _dataland_key in ${keyList}`,
  });

  const trigger_rows = unpackRows(trigger_response);

  const keyGenerator = new KeyGenerator();
  const ordinalGenerator = new OrdinalGenerator();

  // fetch Stripe refunds from Stripe
  const striperefunds = await fetchStriperefunds();

  if (striperefunds == null) {
    return;
  }

  // fetch existing Stripe refunds
  const existing_stripe_data = await querySqlSnapshot({
    logicalTimestamp: transaction.logicalTimestamp,
    sqlQuery: `select
      _dataland_key, id
    from "stripe-refunds"`,
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

  for (const stripeRefund of striperefunds) {
    // Generate an ID
    const id = await keyGenerator.nextKey();
    const ordinal = await ordinalGenerator.nextOrdinal();

    // grab the object_id for each Shippo Shipment
    const stripe_refund_id = String(stripeRefund.id);

    if (stripe_refund_id == null) {
      continue;
    }

    // check if the Stripe refund already exists
    if (existing_stripe_ids.includes(stripe_refund_id)) {
      // get the _dataland_key of the

      const position = existing_stripe_ids.indexOf(stripe_refund_id);
      const existing_key = existing_stripe_keys[position];

      if (!isNumber(existing_key)) {
        continue;
      }

      const update = schema.makeUpdateRows("stripe-refunds", existing_key, {
        id: stripeRefund.id,
        amount: stripeRefund.amount,
        charge: stripeRefund.charge,
        currency: stripeRefund.currency,
        description: stripeRefund.description,
        metadata: JSON.stringify(stripeRefund.metadata),
        payment_intent: stripeRefund.payment_intent,
        reason: stripeRefund.reason,
        status: stripeRefund.status,
      });

      if (update == null) {
        continue;
      }
      mutations_batch.push(update);

      batch_counter++;
      total_counter++;
    } else {
      const insert = schema.makeInsertRows("stripe-refunds", id, {
        _dataland_ordinal: ordinal,
        id: stripeRefund.id,
        amount: stripeRefund.amount,
        charge: stripeRefund.charge,
        currency: stripeRefund.currency,
        description: stripeRefund.description,
        metadata: JSON.stringify(stripeRefund.metadata),
        payment_intent: stripeRefund.payment_intent,
        reason: stripeRefund.reason,
        status: stripeRefund.status,
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
    } else if (total_counter + batch_size > striperefunds.length) {
      await runMutations({ mutations: mutations_batch });
      mutations_batch = [];
      batch_counter = 0;
      console.log("total_counter: ", total_counter);
    }
  }
};

registerTransactionHandler(handler);

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

import { isString } from "lodash-es";

// fetch all of the records from Shippo
// note that this iterates through 23 pages of data

const stripe_key = getEnv("STRIPE_API_KEY");

const fetchStripeCustomers = async () => {
  // --------------------------------------------------
  var myHeaders = new Headers();
  myHeaders.append("Content-Type", "application/x-www-form-urlencoded");
  myHeaders.append("Authorization", `Bearer ${stripe_key}`);

  const full_results = [];

  let url = "https://api.stripe.com//v1/customers?limit=100";
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
        console.log("id: ", result.id);
      }
    }
  } while (has_more);

  return full_results;
};

// --------------------------------------------------

const fetchFirstSubscription = async (customer_id: string) => {
  var myHeaders = new Headers();
  myHeaders.append("Content-Type", "application/x-www-form-urlencoded");
  myHeaders.append("Authorization", `Bearer ${stripe_key}`);

  const response = await fetch(
    "https://api.stripe.com//v1/subscriptions?customer=" + customer_id,
    {
      method: "GET",
      headers: myHeaders,
      redirect: "follow",
    }
  );

  const result = await response.json();
  const first_result_data = result.data[0];
  console.log("subscription:", first_result_data);
  return first_result_data;
};

// --------------------------------------------------

const handler = async (transaction: Transaction) => {
  const { tableDescriptors } = await getCatalogSnapshot({
    logicalTimestamp: transaction.logicalTimestamp,
  });

  const schema = new Schema(tableDescriptors);

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

  const trigger_response = await querySqlSnapshot({
    logicalTimestamp: transaction.logicalTimestamp,
    sqlQuery: `select
      _dataland_key
    from "Import Trigger"
    where _dataland_key in ${keyList}`,
  });

  const trigger_rows = unpackRows(trigger_response);

  const keyGenerator = new KeyGenerator();
  const ordinalGenerator = new OrdinalGenerator();

  // get Stripe customers
  const stripeCustomers = await fetchStripeCustomers();

  if (stripeCustomers == null) {
    return;
  }

  let mutations_batch: Mutation[] = [];
  let batch_counter = 0;
  let batch_size = 100; // push 100 at a time
  let total_counter = 0;

  for (const stripeCustomer of stripeCustomers) {
    // Generate an ID
    const id = await keyGenerator.nextKey();
    const ordinal = await ordinalGenerator.nextOrdinal();

    // grab the object_id for each Shippo Shipment
    const stripe_customer_id = String(stripeCustomer.id);

    if (stripe_customer_id == null) {
      continue;
    }

    const stripe_subscription = await fetchFirstSubscription(
      stripe_customer_id
    );

    const insert = schema.makeInsertRows("stripe-customers", id, {
      _dataland_ordinal: ordinal,
      stripe_customer_id: stripeCustomer.id,
      email: stripeCustomer.email,
      stripe_subscription_id: stripe_subscription.id,
      sub_status: stripe_subscription.status,
      latest_invoice_id: stripe_subscription.latest_invoice,
    });

    if (insert == null) {
      continue;
    }
    mutations_batch.push(insert);

    batch_counter++;
    total_counter++;

    if (batch_counter >= batch_size) {
      await runMutations({ mutations: mutations_batch });
      mutations_batch = [];
      batch_counter = 0;
      console.log("total_counter: ", total_counter);
    }
  }
};

registerTransactionHandler(handler);

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

const fetchStripecards = async () => {
  var myHeaders = new Headers();
  myHeaders.append("Content-Type", "application/x-www-form-urlencoded");
  myHeaders.append("Authorization", `Bearer ${stripe_key}`);

  let total_counter = 0;
  const full_results = [];

  let url = "https://api.stripe.com//v1/cards?limit=100";
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
    "stripe-cards-trigger",
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
    from "stripe-cards-trigger"
    where _dataland_key in ${keyList}`,
  });

  const trigger_rows = unpackRows(trigger_response);

  const keyGenerator = new KeyGenerator();
  const ordinalGenerator = new OrdinalGenerator();

  // fetch Stripe cards from Stripe
  const stripecards = await fetchStripecards();

  if (stripecards == null) {
    return;
  }

  // fetch existing Stripe cards
  const existing_stripe_data = await querySqlSnapshot({
    logicalTimestamp: transaction.logicalTimestamp,
    sqlQuery: `select
      _dataland_key, id
    from "stripe-cards"`,
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

  for (const stripeCard of stripecards) {
    // Generate an ID
    const id = await keyGenerator.nextKey();
    const ordinal = await ordinalGenerator.nextOrdinal();

    // grab the object_id for each Shippo Shipment
    const stripe_card_id = String(stripeCard.id);

    if (stripe_card_id == null) {
      continue;
    }

    // check if the Stripe card already exists
    if (existing_stripe_ids.includes(stripe_card_id)) {
      // get the _dataland_key of the

      const position = existing_stripe_ids.indexOf(stripe_card_id);
      const existing_key = existing_stripe_keys[position];

      if (!isNumber(existing_key)) {
        continue;
      }

      const update = schema.makeUpdateRows("stripe-cards", existing_key, {
        id: stripeCard.id,
        address_city: stripeCard.address_city,
        address_country: stripeCard.address_country,
        address_line1: stripeCard.address_line1,
        address_line2: stripeCard.address_line2,
        address_state: stripeCard.address_state,
        address_zip: stripeCard.address_zip,
        address_zip_check: stripeCard.address_zip_check,
        brand: stripeCard.brand,
        country: stripeCard.country,
        customer: stripeCard.customer,
        cvc_check: stripeCard.cvc_check,
        exp_month: stripeCard.exp_month,
        exp_year: stripeCard.exp_year,
        fingerprint: stripeCard.fingerprint,
        funding: stripeCard.funding,
        last4: stripeCard.last4,
        metadata: stripeCard.metadata,
        name: stripeCard.name,
      });

      if (update == null) {
        continue;
      }
      mutations_batch.push(update);

      batch_counter++;
      total_counter++;
    } else {
      const insert = schema.makeInsertRows("stripe-cards", id, {
        _dataland_ordinal: ordinal,
        id: stripeCard.id,
        address_city: stripeCard.address_city,
        address_country: stripeCard.address_country,
        address_line1: stripeCard.address_line1,
        address_line2: stripeCard.address_line2,
        address_state: stripeCard.address_state,
        address_zip: stripeCard.address_zip,
        address_zip_check: stripeCard.address_zip_check,
        brand: stripeCard.brand,
        country: stripeCard.country,
        customer: stripeCard.customer,
        cvc_check: stripeCard.cvc_check,
        exp_month: stripeCard.exp_month,
        exp_year: stripeCard.exp_year,
        fingerprint: stripeCard.fingerprint,
        funding: stripeCard.funding,
        last4: stripeCard.last4,
        metadata: stripeCard.metadata,
        name: stripeCard.name,
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
    } else if (total_counter + batch_size > stripecards.length) {
      await runMutations({ mutations: mutations_batch });
      mutations_batch = [];
      batch_counter = 0;
      console.log("total_counter: ", total_counter);
    }
  }
};

registerTransactionHandler(handler);

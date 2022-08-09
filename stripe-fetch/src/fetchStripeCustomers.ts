import {
  getCatalogMirror,
  getEnv,
  Mutation,
  querySqlMirror,
  KeyGenerator,
  OrdinalGenerator,
  registerCronHandler,
  runMutations,
  Schema,
  unpackRows,
} from "@dataland-io/dataland-sdk-worker";

import { isNumber } from "lodash-es";

const stripe_key = getEnv("STRIPE_API_KEY");

const fetchStripeCustomers = async () => {
  var headers = new Headers();
  headers.append("Content-Type", "application/x-www-form-urlencoded");
  headers.append("Authorization", `Bearer ${stripe_key}`);

  let total_counter = 0;
  const full_results = [];

  let url = "https://api.stripe.com//v1/customers?limit=100";
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
  const { tableDescriptors } = await getCatalogMirror();

  const schema = new Schema(tableDescriptors);

  const keyGenerator = new KeyGenerator();
  const ordinalGenerator = new OrdinalGenerator();

  // fetch Stripe customers from Stripe
  const stripeCustomers = await fetchStripeCustomers();

  if (stripeCustomers == null) {
    return;
  }

  // fetch existing Stripe customers
  const existing_stripe_data = await querySqlMirror({
    sqlQuery: `select
      _dataland_key, id
    from "stripe-customers"`,
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

  for (const stripeCustomer of stripeCustomers) {
    // Generate a new _dataland_key and _dataland_ordinal value
    const id = await keyGenerator.nextKey();
    const ordinal = await ordinalGenerator.nextOrdinal();

    const stripe_customer_id = String(stripeCustomer.id);

    if (stripe_customer_id == null) {
      continue;
    }

    // check if the Stripe customer already exists
    if (existing_stripe_ids.includes(stripe_customer_id)) {
      const position = existing_stripe_ids.indexOf(stripe_customer_id);
      const existing_key = existing_stripe_keys[position];

      if (!isNumber(existing_key)) {
        continue;
      }

      const update = schema.makeUpdateRows("stripe-customers", existing_key, {
        object: stripeCustomer.object,
        address: stripeCustomer.address,
        balance: stripeCustomer.balance,
        created: stripeCustomer.created,
        currency: stripeCustomer.currency,
        default_currency: stripeCustomer.default_currency,
        default_source: stripeCustomer.default_source,
        delinquent: stripeCustomer.delinquent,
        description: stripeCustomer.description,
        discount: stripeCustomer.discount,
        email: stripeCustomer.email,
        invoice_prefix: stripeCustomer.invoice_prefix,
        livemode: stripeCustomer.livemode,
        metadata: stripeCustomer.metadata,
        name: stripeCustomer.name,
        next_invoice_sequence: stripeCustomer.next_invoice_sequence,
        phone: stripeCustomer.phone,
        preferred_locales: stripeCustomer.preferred_locales,
        shipping: stripeCustomer.shipping,
        tax_exempt: stripeCustomer.tax_exempt,
        test_clock: stripeCustomer.test_clock,
      });

      if (update == null) {
        continue;
      }
      mutations_batch.push(update);

      batch_counter++;
      total_counter++;
    } else {
      const insert = schema.makeInsertRows("stripe-customers", id, {
        _dataland_ordinal: ordinal,
        id: stripeCustomer.id,
        object: stripeCustomer.object,
        address: stripeCustomer.address,
        balance: stripeCustomer.balance,
        created: stripeCustomer.created,
        currency: stripeCustomer.currency,
        default_currency: stripeCustomer.default_currency,
        default_source: stripeCustomer.default_source,
        delinquent: stripeCustomer.delinquent,
        description: stripeCustomer.description,
        discount: stripeCustomer.discount,
        email: stripeCustomer.email,
        invoice_prefix: stripeCustomer.invoice_prefix,
        livemode: stripeCustomer.livemode,
        metadata: stripeCustomer.metadata,
        name: stripeCustomer.name,
        next_invoice_sequence: stripeCustomer.next_invoice_sequence,
        phone: stripeCustomer.phone,
        preferred_locales: stripeCustomer.preferred_locales,
        shipping: stripeCustomer.shipping,
        tax_exempt: stripeCustomer.tax_exempt,
        test_clock: stripeCustomer.test_clock,
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
    } else if (total_counter + batch_size > stripeCustomers.length) {
      await runMutations({ mutations: mutations_batch });
      mutations_batch = [];
      batch_counter = 0;
      console.log("total_counter: ", total_counter);
    }
  }
};

registerCronHandler(handler);

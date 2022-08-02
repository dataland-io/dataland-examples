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

import { isNumber } from "lodash-es";

const stripe_key = getEnv("STRIPE_API_KEY");

const handler = async (transaction: Transaction) => {
  const { tableDescriptors } = await getCatalogSnapshot({
    logicalTimestamp: transaction.logicalTimestamp,
  });

  const schema = new Schema(tableDescriptors);

  const affectedRows = schema.getAffectedRows(
    "view-stripe-subscriptions-with-customers-trigger",
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

  const keyGenerator = new KeyGenerator();
  const ordinalGenerator = new OrdinalGenerator();

  // Check for existing subscription_ids in the database
  const existing_subscriptions = await querySqlSnapshot({
    logicalTimestamp: transaction.logicalTimestamp,
    sqlQuery: `select
      _dataland_key, subscription_id
    from "view-stripe-subscriptions-with-customers"`,
  });

  if (existing_subscriptions == null) {
    return;
  }

  const existing_subscription_rows = unpackRows(existing_subscriptions);

  const existing_stripe_ids = [];
  const existing_stripe_keys = [];

  for (const existing_subscription_row of existing_subscription_rows) {
    existing_stripe_ids.push(existing_subscription_row.subscription_id);
    existing_stripe_keys.push(existing_subscription_row._dataland_key);
  }

  // Construct the new join query
  const joined_query = await querySqlSnapshot({
    logicalTimestamp: transaction.logicalTimestamp,
    sqlQuery: `SELECT
      ss.id as subscription_id,
      ss.status,
      ss.items,
      ss.created,
      sc.id as customer_id,
      sc.email, 
      sc.name,
      sc.phone, 
      sc.delinquent,
      sc.invoice_prefix
    FROM "stripe-subscriptions" ss LEFT JOIN "stripe-customers" sc
    ON ss.customer = sc.id;`,
  });

  if (joined_query == null) {
    return;
  }

  const joined_query_rows = unpackRows(joined_query);

  // batch the mutations
  let mutations_batch: Mutation[] = [];
  let batch_counter = 0;
  let batch_size = 100; // push 100 updates/inserts at a time
  let total_counter = 0;

  for (const joined_query_row of joined_query_rows) {
    const id = await keyGenerator.nextKey();
    const ordinal = await ordinalGenerator.nextOrdinal();

    const subscription_id = joined_query_row.subscription_id;

    // Check if the subscription already exists in the table. if so, update the existing row.
    if (existing_stripe_ids.includes(subscription_id)) {
      console.log("subscription_id already exists: ", subscription_id);
      const position = existing_stripe_ids.indexOf(subscription_id);
      const existing_key = existing_stripe_keys[position];

      if (!isNumber(existing_key)) {
        continue;
      }

      const update = schema.makeUpdateRows(
        "view-stripe-subscriptions-with-customers",
        existing_key,
        {
          subscription_id: joined_query_row.subscription_id,
          status: joined_query_row.status,
          items: joined_query_row.items,
          created: joined_query_row.created,
          customer_id: joined_query_row.customer_id,
          email: joined_query_row.email,
          name: joined_query_row.name,
          phone: joined_query_row.phone,
          delinquent: joined_query_row.delinquent,
          invoice_prefix: joined_query_row.invoice_prefix,
        }
      );

      if (update == null) {
        continue;
      }

      mutations_batch.push(update);

      batch_counter++;
      total_counter++;
    } else {
      // otherwise, make an insert
      const insert = schema.makeInsertRows(
        "view-stripe-subscriptions-with-customers",
        id,
        {
          _dataland_ordinal: ordinal,
          subscription_id: joined_query_row.subscription_id,
          status: joined_query_row.status,
          items: joined_query_row.items,
          created: joined_query_row.created,
          customer_id: joined_query_row.customer_id,
          email: joined_query_row.email,
          name: joined_query_row.name,
          phone: joined_query_row.phone,
          delinquent: joined_query_row.delinquent,
          invoice_prefix: joined_query_row.invoice_prefix,
        }
      );

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
    } else if (total_counter + batch_size > joined_query_rows.length) {
      await runMutations({ mutations: mutations_batch });
      mutations_batch = [];
      batch_counter = 0;
      console.log("total processed: ", total_counter);
    }
  }
};

registerTransactionHandler(handler);

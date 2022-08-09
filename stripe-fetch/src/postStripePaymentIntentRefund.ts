import {
  getCatalogSnapshot,
  getEnv,
  Mutation,
  querySqlSnapshot,
  registerTransactionHandler,
  runMutations,
  Schema,
  Transaction,
  unpackRows,
} from "@dataland-io/dataland-sdk-worker";

import { isString, isNumber } from "lodash-es";

const stripe_key = getEnv("STRIPE_API_KEY");

const postStripeRefund = async (payment_intent_id: string) => {
  var headers = new Headers();
  headers.append("Content-Type", "application/x-www-form-urlencoded");
  headers.append("Authorization", `Bearer ${stripe_key}`);

  const url =
    "https://api.stripe.com/v1/refunds?payment_intent=" + payment_intent_id;

  const options = {
    method: "POST",
    headers: headers,
  };

  const response = await fetch(url, options);
  const result = await response.json();

  console.log("xx - result: ", result);

  return result;
};

// TODO: function is defined, now need to call it from a button

const handler = async (transaction: Transaction) => {
  const { tableDescriptors } = await getCatalogSnapshot({
    logicalTimestamp: transaction.logicalTimestamp,
  });

  const schema = new Schema(tableDescriptors);

  const affectedRows = schema.getAffectedRows(
    "stripe-payment-intents",
    "Issue refund",
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

  // fetch payment_intent_id from stripe_subscription_items
  const stripe_response = await querySqlSnapshot({
    logicalTimestamp: transaction.logicalTimestamp,
    sqlQuery: `select
      _dataland_key, id
    from "stripe-payment-intents" 
    where _dataland_key in ${keyList}`,
  });

  const stripe_rows = unpackRows(stripe_response);

  if (stripe_rows == null) {
    return;
  }

  const mutations: Mutation[] = [];
  for (const stripe_row of stripe_rows) {
    const payment_intent_id = stripe_row.id;
    const key = stripe_row._dataland_key;

    if (!isString(payment_intent_id)) {
      continue;
    }

    if (!isNumber(key)) {
      continue;
    }

    const stripe_response = await postStripeRefund(payment_intent_id);

    if (stripe_response.id == null) {
      continue;
    } else {
      const sentTimestamp = new Date().toISOString();
      const update = schema.makeUpdateRows("stripe-payment-intents", key, {
        "Processed at": sentTimestamp,
        refund_status: stripe_response.status,
      });

      if (update == null) {
        continue;
      }
      mutations.push(update);
    }
  }
  await runMutations({ mutations });
};

registerTransactionHandler(handler);

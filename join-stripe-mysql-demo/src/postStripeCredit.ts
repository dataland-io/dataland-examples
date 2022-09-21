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

// --------------------------------------------------------
// (awu): Define the post Stripe credit function
// --------------------------------------------------------
const postStripeCredit = async (stripe_customer_id: string) => {
  var myHeaders = new Headers();
  myHeaders.append("Content-Type", "application/x-www-form-urlencoded");
  myHeaders.append("Authorization", `Bearer ${stripe_key}`);

  var urlencoded = new URLSearchParams();
  // Issues a $25 credit to the customer
  urlencoded.append("amount", "2500");
  urlencoded.append("currency", "usd");

  const response = await fetch(
    "https://api.stripe.com//v1/customers/" +
      stripe_customer_id +
      "/balance_transactions",
    {
      method: "POST",
      headers: myHeaders,
      body: urlencoded,
      redirect: "follow",
    }
  );

  const result = await response.json();
  return result;
};

// ------------------------------------------------------------
// (awu): This function is invoked by transactions in the Dataland.
//        When users click a button titled "Issue credit" in the Dataland,
//        a transaction is created that invokes this function.
// ------------------------------------------------------------
const handler = async (transaction: Transaction) => {
  const { tableDescriptors } = await getCatalogSnapshot({
    logicalTimestamp: transaction.logicalTimestamp,
  });

  const schema = new Schema(tableDescriptors);

  const affectedRows = schema.getAffectedRows(
    "Orders Credit Workflow",
    "Issue credit",
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

  // -------------------------------------------------------------------
  // (awu): Use Dataland SDK to read the Stripe Customer ID value,
  //        and pass into postStripeCredit function
  // -------------------------------------------------------------------
  const order_response = await querySqlSnapshot({
    logicalTimestamp: transaction.logicalTimestamp,
    sqlQuery: `select
      _dataland_key, "Stripe Customer ID"
    from "Orders Credit Workflow" 
    where _dataland_key in ${keyList}`,
  });

  const order_rows = unpackRows(order_response);

  if (order_rows == null) {
    return;
  }

  const mutations: Mutation[] = [];
  for (const order_row of order_rows) {
    const stripe_customer_id = order_row["Stripe Customer ID"];
    const key = order_row._dataland_key;

    if (!isString(stripe_customer_id)) {
      continue;
    }

    if (!isNumber(key)) {
      continue;
    }

    const credit_response = await postStripeCredit(stripe_customer_id);

    if (credit_response.id == null) {
      continue;
    } else {
      const sentTimestamp = new Date().toISOString();
      const update = schema.makeUpdateRows("Orders Credit Workflow", key, {
        "Credit processed at": sentTimestamp,
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

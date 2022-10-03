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

  const new_quantity = quantity + 1;
  const subscription_item_url_with_qty =
    subscription_item_url + "?quantity=" + new_quantity;
  const post_options = {
    method: "POST",
    headers: headers,
  };

  const post_response = await fetch(
    subscription_item_url_with_qty,
    post_options
  );
  const post_response_json = await post_response.json();

  return post_response_json;
};

const handler = async (transaction: Transaction) => {
  const { tableDescriptors } = await getCatalogSnapshot({
    logicalTimestamp: transaction.logicalTimestamp,
  });

  const schema = new Schema(tableDescriptors);

  const affectedRows = schema.getAffectedRows(
    "stripe_subscription_items",
    "Increment quantity",
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

  // fetch subscription_item_id from stripe_subscription_items
  const stripe_response = await querySqlSnapshot({
    logicalTimestamp: transaction.logicalTimestamp,
    sqlQuery: `select
      _dataland_key, id
    from "stripe_subscription_items"
    where _dataland_key in ${keyList}`,
  });

  const stripe_rows = unpackRows(stripe_response);

  if (stripe_rows == null) {
    return;
  }

  const mutations: Mutation[] = [];
  for (const stripe_row of stripe_rows) {
    const subscription_item_id = stripe_row.id;
    const key = stripe_row._dataland_key;

    if (!isString(subscription_item_id)) {
      continue;
    }

    if (!isNumber(key)) {
      continue;
    }

    const stripe_response = await incrementStripeSubscriptionItemQuantity(
      subscription_item_id
    );

    if (stripe_response.id == null) {
      continue;
    } else {
      const sentTimestamp = new Date().toISOString();
      const update = schema.makeUpdateRows("stripe_subscription_items", key, {
        "Processed at": sentTimestamp,
        quantity: stripe_response.quantity,
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

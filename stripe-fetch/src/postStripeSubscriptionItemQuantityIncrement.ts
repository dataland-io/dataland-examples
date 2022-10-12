import {
  getEnv,
  getHistoryClient,
  getDbClient,
  MutationsBuilder,
  registerTransactionHandler,
  Transaction,
  unpackRows,
} from "@dataland-io/dataland-sdk";

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

// TODO: function is defined, now need to call it from a button
const handler = async (transaction: Transaction) => {
  const affected_row_ids = [];

  for (const mutation of transaction.mutations) {
    if (mutation.kind.oneofKind == "updateRows") {
      if (
        mutation.kind.updateRows.columnNames.includes("increment_quantity") &&
        mutation.kind.updateRows.tableName === "stripe_subscription_items"
      ) {
        for (const row of mutation.kind.updateRows.rows) {
          affected_row_ids.push(row.rowId);
        }
      } else {
        return;
      }
    } else {
      return;
    }
  }

  const affected_row_ids_key_list = affected_row_ids.join(",");

  // get all rows where issue_refund was incremented
  const history = await getHistoryClient();
  const stripe_response = await history.querySqlMirror({
    sqlQuery: `select _row_id, id from "stripe_subscription_items" where _row_id in (${affected_row_ids_key_list})`,
  }).response;

  const stripe_rows = unpackRows(stripe_response);

  if (stripe_rows == null) {
    return;
  }

  for (const stripe_row of stripe_rows) {
    const payment_intent_id = stripe_row.id;

    if (!isString(payment_intent_id) || !isNumber(stripe_row._row_id)) {
      continue;
    }

    const stripe_response = await incrementStripeSubscriptionItemQuantity(
      payment_intent_id
    );

    if (stripe_response.id == null) {
      continue;
    } else {
      const db = await getDbClient();
      await new MutationsBuilder()
        .updateRow("stripe_subscription_items", stripe_row._row_id, {
          processed_at: new Date().toISOString(),
          quantity: stripe_response.quantity,
        })
        .run(db);
    }
  }
};

registerTransactionHandler(handler);

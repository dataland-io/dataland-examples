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
  const affected_row_ids = [];

  for (const mutation of transaction.mutations) {
    if (mutation.kind.oneofKind == "updateRows") {
      if (mutation.kind.updateRows.columnNames.includes("issue_refund")) {
        for (const row of mutation.kind.updateRows.rows) {
          affected_row_ids.push(row.rowId);
        }
      } else {
        return;
      }
    }
  }

  const affected_row_ids_key_list = affected_row_ids.join(",");

  // get all rows where issue_refund was incremented
  const history = await getHistoryClient();
  const stripe_response = await history.querySqlMirror({
    sqlQuery: `select _row_id, id from "stripe_payment_intents" where _row_id in (${affected_row_ids_key_list})`,
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

    const stripe_response = await postStripeRefund(payment_intent_id);

    if (stripe_response.id == null) {
      continue;
    } else {
      const db = await getDbClient();
      await new MutationsBuilder()
        .updateRow("stripe_payment_intents", stripe_row._row_id, {
          processed_at: new Date().toISOString(),
          refund_status: stripe_response.status,
        })
        .run(db);
    }
  }
};

registerTransactionHandler(handler);

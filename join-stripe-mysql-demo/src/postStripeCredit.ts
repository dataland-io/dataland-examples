import {
  getEnv,
  MutationsBuilder,
  getDbClient,
  getHistoryClient,
  registerTransactionHandler,
  Transaction,
  unpackRows,
} from "@dataland-io/dataland-sdk";

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
  const db = await getDbClient();
  const history = await getHistoryClient();

  const affected_row_ids = [];

  // -------------------------------------------------------------------
  // (awu): Get all rows where issue_credit was incremented
  // -------------------------------------------------------------------

  for (const mutation of transaction.mutations) {
    if (mutation.kind.oneofKind == "updateRows") {
      if (
        mutation.kind.updateRows.columnNames.includes("issue_credit") &&
        mutation.kind.updateRows.tableName === "orders_credit_workflow"
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

  // -------------------------------------------------------------------
  // (awu): Grab the stripe_customer_id for each of the issue_credit rows
  // -------------------------------------------------------------------

  const response = await history.querySqlMirror({
    sqlQuery: `select
    _row_id, stripe_customer_id
  from "orders_credit_workflow"
  where _row_id in (${affected_row_ids_key_list})`,
  }).response;

  const rows = unpackRows(response);

  if (rows == null) {
    return;
  }

  // -------------------------------------------------------------------
  // (awu): Use Dataland SDK to read the Stripe Customer ID value,
  //        and pass into postStripeCredit function
  // -------------------------------------------------------------------

  for (const row of rows) {
    const stripe_customer_id = row.stripe_customer_id;
    const row_id = row._row_id;

    if (!isString(stripe_customer_id)) {
      continue;
    }

    if (!isNumber(row_id)) {
      continue;
    }

    const credit_response = await postStripeCredit(stripe_customer_id);

    if (credit_response.id == null) {
      continue;
    } else {
      await new MutationsBuilder()
        .updateRow("orders_credit_workflow", row_id, {
          credit_processed_at: new Date().toISOString(),
        })
        .run(db);
    }
  }
};

registerTransactionHandler(handler);

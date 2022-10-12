import { tableFromJSON, tableToIPC } from "@apache-arrow/es2015-cjs";
import {
  TableSyncRequest,
  getDbClient,
  getEnv,
  registerCronHandler,
} from "@dataland-io/dataland-sdk";

const stripe_key = getEnv("STRIPE_API_KEY");

const fetchStripeRefunds = async () => {
  const headers = new Headers();
  headers.append("Content-Type", "application/x-www-form-urlencoded");
  headers.append("Authorization", `Bearer ${stripe_key}`);

  const full_results = [];

  let url = "https://api.stripe.com//v1/refunds?limit=100";
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
        const stripeRefund = {
          id: result.id,
          amount: result.amount,
          charge: result.charge,
          currency: result.currency,
          description: result.description,
          metadata: JSON.stringify(result.metadata),
          payment_intent: result.payment_intent,
          reason: result.reason,
          status: result.status,
        };
        full_results.push(stripeRefund);
      }
    }
  } while (has_more);

  return full_results;
};

const handler = async () => {
  console.log("fetching Stripe refunds...");
  const records = await fetchStripeRefunds();
  console.log("fetched ", records.length, " Stripe refunds");
  const table = tableFromJSON(records);
  const batch = tableToIPC(table);

  const tableSyncRequest: TableSyncRequest = {
    tableName: "stripe_refunds",
    arrowRecordBatches: [batch],
    primaryKeyColumnNames: ["id"],
    dropExtraColumns: false,
    deleteExtraRows: true,
    transactionAnnotations: {},
    tableAnnotations: {},
    columnAnnotations: {},
  };

  const db = getDbClient();
  await db.tableSync(tableSyncRequest);
  console.log("synced Stripe refunds to Dataland");
};

registerCronHandler(handler);

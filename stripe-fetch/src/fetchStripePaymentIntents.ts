import { tableFromJSON, tableToIPC } from "@apache-arrow/es2015-cjs";
import {
  TableSyncRequest,
  getDbClient,
  getEnv,
  registerCronHandler,
} from "@dataland-io/dataland-sdk";

const stripe_key = getEnv("STRIPE_API_KEY");

const fetchStripePaymentIntents = async () => {
  const headers = new Headers();
  headers.append("Content-Type", "application/x-www-form-urlencoded");
  headers.append("Authorization", `Bearer ${stripe_key}`);

  const full_results = [];

  let url = "https://api.stripe.com//v1/payment_intents?limit=100";
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
        const stripePaymentIntent = {
          id: result.id,
          amount: result.amount,
          automatic_payment_methods: result.automatic_payment_methods,
          client_secret: result.client_secret,
          currency: result.currency,
          customer: result.customer,
          description: result.description,
          last_payment_error: result.last_payment_error,
          metadata: JSON.stringify(result.metadata),
          next_action: result.next_action,
          payment_method: result.payment_method,
          payment_method_types: result.payment_method_types.join(","),
          receipt_email: result.receipt_email,
          setup_future_usage: result.setup_future_usage,
          shipping: result.shipping,
          statement_descriptor: result.statement_descriptor,
          statement_descriptor_suffix: result.statement_descriptor_suffix,
          status: result.status,
        };
        full_results.push(stripePaymentIntent);
      }
    }
  } while (has_more);

  return full_results;
};

const handler = async () => {
  console.log("Fetching Stripe payment intents...");
  const records = await fetchStripePaymentIntents();
  console.log("Fetched ", records.length, " Stripe payment intents");
  const table = tableFromJSON(records);
  const batch = tableToIPC(table);

  const tableSyncRequest: TableSyncRequest = {
    tableName: "stripe_payment_intents",
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
  console.log("Synced Stripe payment intents to Dataland");
};

registerCronHandler(handler);

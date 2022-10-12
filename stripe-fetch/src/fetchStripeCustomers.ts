import { tableFromJSON, tableToIPC } from "@apache-arrow/es2015-cjs";
import {
  TableSyncRequest,
  getEnv,
  registerCronHandler,
  getDbClient,
} from "@dataland-io/dataland-sdk";

const stripe_key = getEnv("STRIPE_API_KEY");

const fetchStripeCustomers = async () => {
  const headers = new Headers();
  headers.append("Content-Type", "application/x-www-form-urlencoded");
  headers.append("Authorization", `Bearer ${stripe_key}`);

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
        const stripeCustomer = {
          id: result.id,
          object: result.object,
          address: result.address,
          balance: result.balance,
          created: result.created,
          currency: result.currency,
          default_currency: result.default_currency,
          default_source: result.default_source,
          delinquent: result.delinquent,
          description: result.description,
          discount: result.discount,
          email: result.email,
          invoice_prefix: result.invoice_prefix,
          livemode: result.livemode,
          metadata: JSON.stringify(result.metadata),
          name: result.name,
          next_invoice_sequence: result.next_invoice_sequence,
          phone: result.phone,
          shipping: result.shipping,
          tax_exempt: result.tax_exempt,
        };
        full_results.push(stripeCustomer);
      }
    }
  } while (has_more);

  return full_results;
};

const handler = async () => {
  console.log("fetching Stripe customers...");
  const records = await fetchStripeCustomers();
  console.log("fetched ", records.length, " Stripe customers");

  const table = tableFromJSON(records);
  const batch = tableToIPC(table);

  const tableSyncRequest: TableSyncRequest = {
    tableName: "stripe_customers",
    arrowRecordBatches: [batch],
    primaryKeyColumnNames: ["id"],
    dropExtraColumns: true,
    deleteExtraRows: true,
    transactionAnnotations: {},
    tableAnnotations: {},
    columnAnnotations: {},
  };

  const db = getDbClient();
  await db.tableSync(tableSyncRequest);
  console.log("synced Stripe customers to Dataland");
};

registerCronHandler(handler);

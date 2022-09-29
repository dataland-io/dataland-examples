import {
  getEnv,
  syncTables,
  SyncTable,
  registerCronHandler,
} from "@dataland-io/dataland-sdk-worker";

import { tableFromJSON, tableToIPC } from "@apache-arrow/es2015-esm";

const stripe_key = getEnv("STRIPE_API_KEY");

const fetchStripeInvoices = async () => {
  var headers = new Headers();
  headers.append("Content-Type", "application/x-www-form-urlencoded");
  headers.append("Authorization", `Bearer ${stripe_key}`);

  const full_results = [];

  let url = "https://api.stripe.com//v1/invoices?limit=100";
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
        const stripeInvoice = {
          id: result.id,
          auto_advance: result.auto_advance,
          charge: result.charge,
          collection_method: result.collection_method,
          currency: result.currency,
          customer: result.customer,
          description: result.description,
          hosted_invoice_url: result.hosted_invoice_url,
          metadata: JSON.stringify(result.metadata),
          payment_intent: result.payment_intent,
          period_end: result.period_end,
          period_start: result.period_start,
          status: result.status,
          subscription: result.subscription,
          total: result.total,
        };
        full_results.push(stripeInvoice);
      }
    }
  } while (has_more);

  return full_results;
};

const handler = async () => {
  console.log("fetching Stripe invoices...");
  const records = await fetchStripeInvoices();
  console.log("fetched ", records.length, " Stripe invoices");
  const table = tableFromJSON(records);
  const batch = tableToIPC(table);

  const syncTable: SyncTable = {
    tableName: "stripe_invoices",
    arrowRecordBatches: [batch],
    identityColumnNames: ["id"],
  };

  await syncTables({ syncTables: [syncTable] });
  console.log("synced Stripe invoices to Dataland");
};

registerCronHandler(handler);

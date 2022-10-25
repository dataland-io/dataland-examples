import { tableFromJSON, tableToIPC } from "apache-arrow";
import {
  TableSyncRequest,
  getDbClient,
  getEnv,
  registerCronHandler,
} from "@dataland-io/dataland-sdk";

const fetchShopifyCustomers = async (
  SHOPIFY_STORE_NAME: string,
  SHOPIFY_ACCESS_TOKEN: string
) => {
  const url =
    "https://" +
    SHOPIFY_STORE_NAME +
    ".myshopify.com/admin/api/2022-10/customers.json";

  const options = {
    method: "GET",
    headers: {
      "Content-Type": "application/json",
      "X-Shopify-Access-Token": `${SHOPIFY_ACCESS_TOKEN}`,
    },
  };

  const response = await fetch(url, options);
  const result = await response.json();
  const customers = result.customers;

  const customers_trimmed = [];
  // const customers_patients = [];

  for (const customer of customers) {
    const customer_trimmed = {
      id: customer.id,
      email: customer.email,
      accepts_marketing: customer.accepts_marketing,
      created_at: customer.created_at,
      updated_at: customer.updated_at,
      first_name: customer.first_name,
      last_name: customer.last_name,
      orders_count: customer.orders_count,
      state: customer.state,
      total_spent: Number(customer.total_spent),
      last_order_id: customer.last_order_id,
      note: customer.note,
      verified_email: customer.verified_email,
      multipass_identifier: customer.multipass_identifier,
      tax_exempt: customer.tax_exempt,
      tags: customer.tags,
      last_order_name: customer.last_order_name,
      currency: customer.currency,
      default_address: JSON.stringify(customer.default_address),
    };
    customers_trimmed.push(customer_trimmed);
  }
  return {
    full_table: customers_trimmed,
  };
};

const cronHandler = async () => {
  const db = getDbClient();

  const SHOPIFY_STORE_NAME = getEnv("SHOPIFY_STORE_NAME");

  if (SHOPIFY_STORE_NAME == null) {
    throw new Error("Missing environment variable - SHOPIFY_STORE_NAME");
  }

  const SHOPIFY_ACCESS_TOKEN = getEnv("SHOPIFY_ACCESS_TOKEN");

  if (SHOPIFY_ACCESS_TOKEN == null) {
    throw new Error("Missing environment variable - SHOPIFY_ACCESS_TOKEN");
  }

  const shopify_data = await fetchShopifyCustomers(
    SHOPIFY_STORE_NAME,
    SHOPIFY_ACCESS_TOKEN
  );

  const full_table = tableFromJSON(shopify_data.full_table);
  const full_batch = tableToIPC(full_table);

  const shopifyCustomerTableSyncRequest: TableSyncRequest = {
    tableName: "shopify_customers",
    arrowRecordBatches: [full_batch],
    primaryKeyColumnNames: ["id"],
    dropExtraColumns: true,
    deleteExtraRows: true,
    transactionAnnotations: {},
    tableAnnotations: {},
    columnAnnotations: {},
  };

  await db.tableSync(shopifyCustomerTableSyncRequest);
};

registerCronHandler(cronHandler);

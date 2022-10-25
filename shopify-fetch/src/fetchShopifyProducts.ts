import { tableFromJSON, tableToIPC } from "apache-arrow";
import {
  TableSyncRequest,
  getDbClient,
  getEnv,
  registerCronHandler,
} from "@dataland-io/dataland-sdk";

const fetchShopifyProducts = async (
  SHOPIFY_STORE_NAME: string,
  SHOPIFY_ACCESS_TOKEN: string
) => {
  const url =
    "https://" +
    SHOPIFY_STORE_NAME +
    ".myshopify.com/admin/api/2022-10/products.json";

  const options = {
    method: "GET",
    headers: {
      "Content-Type": "application/json",
      "X-Shopify-Access-Token": `${SHOPIFY_ACCESS_TOKEN}`,
    },
  };

  const response = await fetch(url, options);
  const result = await response.json();
  const products = result.products;

  const products_trimmed = [];

  for (const product of products) {
    const product_trimmed = {
      id: product.id,
      title: product.title,
      body_html: product.body_html,
      vendor: product.vendor,
      product_type: product.product_type,
      created_at: product.created_at,
      handle: product.handle,
      updated_at: product.updated_at,
      published_at: product.published_at,
      template_suffix: product.template_suffix,
      status: product.status,
      tags: product.tags,
      admin_graphql_api_id: product.admin_graphql_api_id,
      variants: JSON.stringify(product.variants),
    };
    products_trimmed.push(product_trimmed);
  }
  return products_trimmed;
};

const cronHandler = async () => {
  const SHOPIFY_STORE_NAME = getEnv("SHOPIFY_STORE_NAME");

  if (SHOPIFY_STORE_NAME == null) {
    throw new Error("Missing environment variable - SHOPIFY_STORE_NAME");
  }

  const SHOPIFY_ACCESS_TOKEN = getEnv("SHOPIFY_ACCESS_TOKEN");

  if (SHOPIFY_ACCESS_TOKEN == null) {
    throw new Error("Missing environment variable - SHOPIFY_ACCESS_TOKEN");
  }

  const shopify_data = await fetchShopifyProducts(
    SHOPIFY_STORE_NAME,
    SHOPIFY_ACCESS_TOKEN
  );

  const table = tableFromJSON(shopify_data);
  const batch = tableToIPC(table);

  const db = getDbClient();

  const TableSyncRequest: TableSyncRequest = {
    tableName: "shopify_products",
    arrowRecordBatches: [batch],
    primaryKeyColumnNames: ["id"],
    dropExtraColumns: false,
    deleteExtraRows: true,
    transactionAnnotations: {},
    tableAnnotations: {},
    columnAnnotations: {},
  };

  await db.tableSync(TableSyncRequest);
};

registerCronHandler(cronHandler);

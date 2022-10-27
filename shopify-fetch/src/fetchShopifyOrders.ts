import { tableFromJSON, tableToIPC } from "apache-arrow";
import {
  TableSyncRequest,
  getDbClient,
  getEnv,
  registerCronHandler,
} from "@dataland-io/dataland-sdk";

const SHOPIFY_ACCESS_TOKEN = getEnv("SHOPIFY_ACCESS_TOKEN");

if (SHOPIFY_ACCESS_TOKEN == null) {
  throw new Error("Missing environment variable - SHOPIFY_ACCESS_TOKEN");
}

const fetchShopifyOrders = async (
  SHOPIFY_STORE_NAME: string,
  SHOPIFY_ACCESS_TOKEN: string
) => {
  const url_base =
    "https://" +
    SHOPIFY_STORE_NAME +
    ".myshopify.com/admin/api/2022-10/orders.json?status=any&limit=250";

  const options = {
    method: "GET",
    headers: {
      "Content-Type": "application/json",
      "X-Shopify-Access-Token": `${SHOPIFY_ACCESS_TOKEN}`,
    },
  };

  const orders_trimmed = [];
  const order_line_items_trimmed = [];
  let last_id_operator = "";
  let url = url_base + last_id_operator;
  let has_more = false;

  do {
    const response = await fetch(url, options);
    const result = await response.json();
    const result_headers = response.headers;
    // parse link inside angle brackets
    const link = result_headers.get("link");
    // split link into array
    if (link != null) {
      const link_array = link.split(",");
      // for each link in array, split by semicolon
      for (const link_item of link_array) {
        const link_item_array = link_item.split(";");
        const link_item_link = link_item_array[0];
        const link_item_rel = link_item_array[1];
        if (link_item_rel.includes("next")) {
          // parse link inside angle brackets
          const link_item_link_array = link_item_link
            .split(">")[0]
            .split("<")[1];
          console.log("xx- link_item_link_array_next", link_item_link_array);
          url = link_item_link_array;
          has_more = true;
        } else {
          has_more = false;
        }
      }
    } else {
      has_more = false;
    }

    if (result == null) {
      break;
    }

    const orders = result.orders;

    if (orders == null) {
      break;
    }

    for (const order of orders) {
      const order_trimmed = {
        id: order.id,
        customer_id: order.customer?.id ?? "",
        financial_status: order.financial_status,
        created_at: order.created_at,
        total_price: order.total_price,
        total_discounts: order.total_discounts,
        total_line_items_price: order.total_line_items_price,
        total_tax: order.total_tax,
        total_price_usd: order.total_price_usd,
        total_discounts_usd: order.total_discounts_usd,
        total_line_items_price_usd: order.total_line_items_price_usd,
        total_tax_usd: order.total_tax_usd,
        subtotal_price: order.subtotal_price,
        raw_shipping_address: JSON.stringify(order.shipping_address),
        subtotal_price_usd: order.subtotal_price_usd,
        total_weight: order.total_weight,
        currency: order.currency,
        customer: JSON.stringify(order.customer),
        line_items: JSON.stringify(order.line_items),
      };

      const line_items = order.line_items;

      for (const line_item of line_items) {
        const line_item_trimmed = {
          id: line_item.id,
          order_id: order.id,
          name: line_item.name,
          quantity: line_item.quantity,
          price: line_item.price,
          properties: JSON.stringify(line_item.properties),
          requires_shipping: line_item.requires_shipping,
          total_discount: line_item.total_discount,
          raw_shipping_address: JSON.stringify(order.shipping_address),
          variant_id: line_item.variant_id,
          product_id: line_item.product_id,
        };
        order_line_items_trimmed.push(line_item_trimmed);
      }
      orders_trimmed.push(order_trimmed);
      last_id_operator = "&since_id=" + order.id;
    }
    console.log("Read in order count", orders_trimmed.length);
    console.log(
      "Read in order line item count",
      order_line_items_trimmed.length
    );
    console.log("Has more", has_more);
  } while (has_more);

  console.log("outside loop: Read in order count", orders_trimmed.length);
  return {
    orders_trimmed: orders_trimmed,
    order_line_items_trimmed: order_line_items_trimmed,
  };
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

  const shopify_data = await fetchShopifyOrders(
    SHOPIFY_STORE_NAME,
    SHOPIFY_ACCESS_TOKEN
  );

  if (shopify_data == null) {
    return;
  }

  console.log("orders_length", shopify_data.orders_trimmed.length);

  const orders_table = tableFromJSON(shopify_data.orders_trimmed);
  const orders_batch = tableToIPC(orders_table);

  const ordersTableSyncRequest: TableSyncRequest = {
    tableName: "shopify_orders",
    arrowRecordBatches: [orders_batch],
    primaryKeyColumnNames: ["id"],
    dropExtraColumns: false,
    deleteExtraRows: true,
    transactionAnnotations: {},
    tableAnnotations: {},
    columnAnnotations: {},
  };

  const order_line_items_table = tableFromJSON(
    shopify_data.order_line_items_trimmed
  );
  const order_line_items_batch = tableToIPC(order_line_items_table);

  const orderLineItemsTableSyncRequest: TableSyncRequest = {
    tableName: "shopify_order_line_items",
    arrowRecordBatches: [order_line_items_batch],
    primaryKeyColumnNames: ["id"],
    dropExtraColumns: false,
    deleteExtraRows: true,
    transactionAnnotations: {},
    tableAnnotations: {},
    columnAnnotations: {},
  };

  const db = getDbClient();

  await db.tableSync(ordersTableSyncRequest);
  await db.tableSync(orderLineItemsTableSyncRequest);
};

registerCronHandler(cronHandler);

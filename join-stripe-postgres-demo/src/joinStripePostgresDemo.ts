import {
  querySqlMirror,
  registerCronHandler,
  syncTables,
  SyncTable,
  unpackRows,
} from "@dataland-io/dataland-sdk-worker";

const handler = async () => {
  // Construct the new join query
  const joined_query = await querySqlMirror({
    sqlQuery: `
      WITH total_order_value_by_customer AS (
          SELECT
            po.customer_id,
            SUM(po."order_value") as "total_order_value"
          FROM "postgres-orders" po
          GROUP BY ("customer_id")
        )
        SELECT
          po.id AS "Order ID",
          po.customer_id AS "Customer ID",
          po.delivery_status AS "Delivery Status",
          po.order_placed_at AS "Order Placed At",
          po.order_delivered_at AS "Order Delivered At",
          po.delivery_time AS "Delivery Time (mins)",
          po.rating AS "User Rating",
          po.user_rating_comment AS "User Rating Comment",
          po.order_value AS "Order Value",
          pu.phone AS "Phone",
          pu.email AS "Email",
          pu.name AS "Name",
          tovbc.total_order_value AS "Lifetime Order Value",
          pu.stripe_customer_id as "Stripe Customer ID"
        FROM
          "postgres-orders" po
        LEFT JOIN
          "postgres-users" pu ON po.customer_id = pu.id
        LEFT JOIN
            "total_order_value_by_customer" tovbc ON tovbc.customer_id = pu.id;
    `,
  });

  if (joined_query == null) {
    return;
  }

  const syncTable: SyncTable = {
    tableName: "Alerts on Orders",
    arrowRecordBatches: joined_query.arrowRecordBatches,
    identityColumnNames: ["Order ID"],
    keepExtraColumns: true,
  };
  try {
    await syncTables({
      syncTables: [syncTable],
    });
    console.log("Sync complete");
  } catch (e) {
    console.warn(`syncTables failed`, e);
  }
};

registerCronHandler(handler);

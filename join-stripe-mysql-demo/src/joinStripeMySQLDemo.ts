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
            mso.customer_id,
            SUM(mso."order_value") as "total_order_value"
          FROM "MySQL Orders" mso
          GROUP BY ("customer_id")
        )
        SELECT
          mso.order_id AS "Order ID",
          mso.customer_id AS "Customer ID",
          mso.delivery_status AS "Delivery Status",
          mso.order_placed_at AS "Order Placed At",
          mso.delivery_time AS "Delivery Time (mins)",
          mso.rating AS "User Rating",
          mso.user_rating_comment AS "User Rating Comment",
          mso.order_value AS "Order Value",
          msu.phone AS "Phone",
          msu.email AS "Email",
          msu.name AS "Name",
          tovbc.total_order_value AS "Lifetime Order Value",
          msu.stripe_customer_id AS "Stripe Customer ID",
          CONCAT('https://dashboard.stripe.com/test/customers/',msu.stripe_customer_id) as "Stripe URL"
        FROM
          "MySQL Orders" mso
        LEFT JOIN
          "MySQL Users" msu ON mso.customer_id = msu.id
        LEFT JOIN
            "total_order_value_by_customer" tovbc ON tovbc.customer_id = msu.id;
    `,
  });

  if (joined_query == null) {
    return;
  }

  const syncTable: SyncTable = {
    tableName: "Orders Credit Workflow",
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

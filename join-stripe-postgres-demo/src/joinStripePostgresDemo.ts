import {
  querySqlMirror,
  registerCronHandler,
  syncTables,
  SyncTable,
  unpackRows,
} from "@dataland-io/dataland-sdk-worker";

import { tableFromJSON, tableToIPC } from "@apache-arrow/es2015-esm";
const handler = async () => {
  // Construct the new join query
  const joined_query = await querySqlMirror({
    sqlQuery: `WITH total_order_value_by_customer AS (
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

  const joined_query_rows = unpackRows(joined_query);
  const arrowTable = tableFromJSON(joined_query_rows);
  // batch the mutations
  const arrowRecordBatch = tableToIPC(arrowTable);
  const syncTable: SyncTable = {
    tableName: "humans",
    arrowRecordBatches: [arrowRecordBatch],
    identityColumnNames: ["id"],
  };
  try {
    await syncTables({
      syncTables: [syncTable],
    });
  } catch (e) {
    console.warn(`syncTables failed`, e);
  }
  // for (const joined_query_row of joined_query_rows) {
  //   let rating = joined_query_row.rating;
  //   if (Number.isNaN(joined_query_row.rating)) {
  //     rating = null;
  //   }

  //   let delivery_time = joined_query_row.delivery_time;
  //   if (Number.isNaN(joined_query_row.delivery_time)) {
  //     delivery_time = null;
  //   }
  // }

  // Check if the subscription already exists in the table. if so, update the existing row.
};

registerCronHandler(handler);

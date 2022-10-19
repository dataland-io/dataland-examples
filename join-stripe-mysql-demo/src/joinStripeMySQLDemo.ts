import {
  registerCronHandler,
  getDbClient,
  getHistoryClient,
  TableSyncRequest,
} from "@dataland-io/dataland-sdk";

const handler = async () => {
  // Construct the new join query
  const db = await getDbClient();
  const history = await getHistoryClient();

  // --------------------------------------------------------
  // (awu): Define the "orders_credit_workflow" table
  // --------------------------------------------------------
  const joined_query = await history.querySqlMirror({
    sqlQuery: `
    WITH total_order_value_by_customer AS (
        SELECT
          mso.customer_id,
          SUM(mso."order_value") as "total_order_value"
        FROM "mysql_orders" mso
        GROUP BY ("customer_id")
      )
      SELECT
        mso.order_id AS "order_id",
        mso.customer_id AS "customer_id",
        mso.delivery_status AS "delivery_status",
        mso.order_placed_at AS "order_placed_at",
        mso.delivery_time AS "delivery_time_mins",
        mso.rating AS "user_rating",
        mso.user_rating_comment AS "user_rating_comment",
        mso.order_value AS "order_value",
        msu.phone AS "phone",
        msu.email AS "email",
        msu.name AS "name",
        tovbc.total_order_value AS "lifetime_order_value",
        msu.stripe_customer_id AS "stripe_customer_id",
        CONCAT('https://dashboard.stripe.com/test/customers/',msu.stripe_customer_id) as "stripe_url"
      FROM
        "mysql_orders" mso
      LEFT JOIN
        "mysql_users" msu ON mso.customer_id = msu.id
      LEFT JOIN
          "total_order_value_by_customer" tovbc ON tovbc.customer_id = msu.id;
  `,
  }).response;

  if (joined_query == null) {
    return;
  }

  // --------------------------------------------------------
  // (awu): Write the data to Dataland
  // --------------------------------------------------------
  const tableSyncRequest: TableSyncRequest = {
    tableName: "orders_credit_workflow",
    arrowRecordBatches: joined_query.arrowRecordBatches,
    primaryKeyColumnNames: ["order_id"],
    dropExtraColumns: false,
    deleteExtraRows: true,
    transactionAnnotations: {},
    tableAnnotations: {},
    columnAnnotations: {},
  };

  await db.tableSync(tableSyncRequest);
};

registerCronHandler(handler);

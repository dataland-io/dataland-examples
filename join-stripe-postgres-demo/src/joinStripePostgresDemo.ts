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

  // const joined_query = await querySqlMirror({
  //   sqlQuery: `
  //       SELECT
  //         po.id AS "Order ID",
  //         po.customer_id AS "Customer ID",
  //         po.delivery_status AS "Delivery Status",
  //         po.order_placed_at AS "Order Placed At",
  //         po.order_delivered_at AS "Order Delivered At",
  //         po.delivery_time AS "Delivery Time (mins)",
  //         po.rating AS "User Rating",
  //         po.user_rating_comment AS "User Rating Comment",
  //         po.order_value AS "Order Value",
  //         pu.phone AS "Phone",
  //         pu.email AS "Email",
  //         pu.name AS "Name",
  //         pu.stripe_customer_id as "Stripe Customer ID"
  //       FROM
  //         "postgres-orders" po
  //       LEFT JOIN
  //         "postgres-users" pu ON po.customer_id = pu.id;
  //   `,
  // });

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

// import {
//   getCatalogMirror,
//   Mutation,
//   querySqlMirror,
//   KeyGenerator,
//   OrdinalGenerator,
//   registerCronHandler,
//   runMutations,
//   Schema,
//   unpackRows,
// } from "@dataland-io/dataland-sdk-worker";

// import { isNumber } from "lodash-es";

// const handler = async () => {
//   const { tableDescriptors } = await getCatalogMirror();

//   const schema = new Schema(tableDescriptors);

//   const keyGenerator = new KeyGenerator();
//   const ordinalGenerator = new OrdinalGenerator();

//   // Check for existing subscription_ids in the database
//   const existing_rows_response = await querySqlMirror({
//     sqlQuery: `select
//       _dataland_key, "Order ID"
//     from "Alerts on Orders"`,
//   });

//   if (existing_rows_response == null) {
//     return;
//   }

//   const existing_rows = unpackRows(existing_rows_response);

//   const existing_stripe_ids = [];
//   const existing_stripe_keys = [];

//   for (const existing_row of existing_rows) {
//     existing_stripe_ids.push(existing_row["Order ID"]);
//     existing_stripe_keys.push(existing_row._dataland_key);
//   }

//   // Construct the new join query
//   const joined_query = await querySqlMirror({
//     sqlQuery: `WITH total_order_value_by_customer AS (
//       SELECT
//         po.customer_id,
//         SUM(po."order_value") as "total_order_value"
//       FROM "postgres-orders" po
//       GROUP BY ("customer_id")
//     )
//     SELECT
//       po.*,
//       pu.phone,
//       pu.email,
//       pu.name,
//       tovbc.total_order_value,
//       pu.stripe_customer_id
//     FROM
//       "postgres-orders" po
//     LEFT JOIN
//       "postgres-users" pu ON po.customer_id = pu.id
//     LEFT JOIN
//         "total_order_value_by_customer" tovbc ON tovbc.customer_id = pu.id;
// `,
//   });

//   if (joined_query == null) {
//     return;
//   }

//   const joined_query_rows = unpackRows(joined_query);

//   // batch the mutations
//   let mutations_batch: Mutation[] = [];
//   let batch_counter = 0;
//   let batch_size = 100; // push 100 updates/inserts at a time
//   let total_counter = 0;

//   for (const joined_query_row of joined_query_rows) {
//     const id = await keyGenerator.nextKey();
//     const ordinal = await ordinalGenerator.nextOrdinal();

//     const subscription_id = joined_query_row.subscription_id;

//     const stripe_customer_id = joined_query_row.stripe_customer_id;

//     const stripe_url =
//       "https://dashboard.stripe.com/test/customers/" + stripe_customer_id;

//     // Check if the subscription already exists in the table. if so, update the existing row.
//     if (existing_stripe_ids.includes(subscription_id)) {
//       console.log("subscription_id already exists: ", subscription_id);
//       const position = existing_stripe_ids.indexOf(subscription_id);
//       const existing_key = existing_stripe_keys[position];

//       if (!isNumber(existing_key)) {
//         continue;
//       }

//       const update = schema.makeUpdateRows("Alerts on Orders", existing_key, {
//         "Order ID": joined_query_row.id,
//         "Customer ID": joined_query_row.customer_id,
//         "Delivery Status": joined_query_row.delivery_status,
//         "Order Placed At": joined_query_row.order_placed_at,
//         "Order Delivered At": joined_query_row.order_delivered_at,
//         "Delivery Time (mins)": joined_query_row.delivery_time,
//         "User Rating": joined_query_row.rating,
//         "User Rating Comment": joined_query_row.rating_comment,
//         "Order Value": joined_query_row.order_value,
//         Phone: joined_query_row.phone,
//         Email: joined_query_row.email,
//         Name: joined_query_row.name,
//         "Lifetime Order Value": joined_query_row.total_order_value,
//         "Stripe Customer ID": joined_query_row.stripe_customer_id,
//         "Stripe URL": stripe_url,
//       });

//       if (update == null) {
//         continue;
//       }

//       mutations_batch.push(update);

//       batch_counter++;
//       total_counter++;
//     } else {
//       // otherwise, make an insert
//       const insert = schema.makeInsertRows("Alerts on Orders", id, {
//         _dataland_ordinal: ordinal,
//         "Order ID": joined_query_row.id,
//         "Customer ID": joined_query_row.customer_id,
//         "Delivery Status": joined_query_row.delivery_status,
//         "Order Placed At": joined_query_row.order_placed_at,
//         "Order Delivered At": joined_query_row.order_delivered_at,
//         "Delivery Time (mins)": joined_query_row.delivery_time,
//         "User Rating": joined_query_row.rating,
//         "User Rating Comment": joined_query_row.rating_comment,
//         "Order Value": joined_query_row.order_value,
//         Phone: joined_query_row.phone,
//         Email: joined_query_row.email,
//         Name: joined_query_row.name,
//         "Lifetime Order Value": joined_query_row.total_order_value,
//         "Stripe Customer ID": joined_query_row.stripe_customer_id,
//         "Stripe URL": stripe_url,
//       });

//       if (insert == null) {
//         continue;
//       }

//       mutations_batch.push(insert);

//       batch_counter++;
//       total_counter++;
//     }

//     if (batch_counter >= batch_size) {
//       await runMutations({ mutations: mutations_batch });
//       mutations_batch = [];
//       batch_counter = 0;
//       console.log("total processed: ", total_counter);
//     } else if (total_counter + batch_size > joined_query_rows.length) {
//       await runMutations({ mutations: mutations_batch });
//       mutations_batch = [];
//       batch_counter = 0;
//       console.log("total processed: ", total_counter);
//     }
//   }
// };

// registerCronHandler(handler);

import {
  getCatalogSnapshot,
  getEnv,
  Mutation,
  KeyGenerator,
  querySqlSnapshot,
  registerTransactionHandler,
  runMutations,
  Schema,
  Transaction,
  unpackRows,
} from "@dataland-io/dataland-sdk-worker";
import { isNumber } from "lodash-es";
import * as t from "io-ts";

const STRAPI_ENDPOINT = "https://strapi-cms-example.herokuapp.com/api/items";
const STRAPI_LINK_BASE =
  "https://tranquil-caverns-46219.herokuapp.com/admin/plugins/content-manager/collectionType/application::offers.offers";

const fetchCmsEntry = async (cms_id: number) => {
  const url = `${STRAPI_ENDPOINT}/${cms_id}`;

  const options = {
    method: "GET",
    headers: {
      "Content-Type": "application/json",
    },
  };

  const response = await fetch(url, options);
  const result = await response.json();
  console.log("GET result", result);
  return result;
};

const updateCmsEntry = async (
  strapi_cms_id: number,
  product_name: string,
  price: number,
  category: string,
  product_image_url: string
) => {
  const url = `${STRAPI_ENDPOINT}/${strapi_cms_id}`;

  const options = {
    method: "PUT",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      product_name,
      price,
      category,
      product_image_url,
    }),
  };

  const response = await fetch(url, options);
  const result = await response.json();
  console.log("PUT result", result);
  return result;
};

const createCmsEntry = async (
  product_name: string,
  price: number,
  category: string,
  product_image_url: string
) => {
  const url = `${STRAPI_ENDPOINT}`;

  const options = {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      product_name,
      price,
      category,
      product_image_url,
    }),
  };

  const response = await fetch(url, options);
  const result = await response.json();
  console.log("POST result", result);
  return result;
};

const handler = async (transaction: Transaction) => {
  // only run if Push to CMS button is clicked

  const { tableDescriptors } = await getCatalogSnapshot({
    logicalTimestamp: transaction.logicalTimestamp,
  });
  const schema = new Schema(tableDescriptors);

  const affectedRows = schema.getAffectedRows(
    "Strapi Items",
    "Push to CMS",
    transaction
  );

  const strapiItemKeys: number[] = [];
  for (const [key, value] of affectedRows) {
    if (typeof value === "number" && value > 0) {
      strapiItemKeys.push(key);
    }
  }

  if (strapiItemKeys.length === 0) {
    return;
  }

  const keyList = `(${strapiItemKeys.join(",")})`;
  const response = await querySqlSnapshot({
    logicalTimestamp: transaction.logicalTimestamp,
    sqlQuery: `
      select
        _dataland_key,
        "Product Name" as product_name,
        Price as price,
        Category as category,
        "Product Image URL" as product_image_url,
        "Strapi CMS ID" as strapi_cms_id
      from "Strapi Items"
      where _dataland_key in ${keyList}
    `,
  });

  const rows = unpackRows(response);

  const RowT = t.type({
    _dataland_key: t.number,
    product_name: t.string,
    price: t.number,
    category: t.string,
    product_image_url: t.string,
  });

  const keyGenerator = new KeyGenerator();

  const mutations: Mutation[] = [];
  for (const row of rows) {
    const row_core = {
      _dataland_key: row._dataland_key,
      product_name: row.product_name,
      price: row.price,
      category: row.category,
      product_image_url: row.product_image_url,
    };

    if (!RowT.is(row_core)) {
      continue;
    }

    // If CMS ID exists in the CMS, update the entry
    if (isNumber(row.strapi_cms_id)) {
      const strapi_cms_entry = await fetchCmsEntry(row.strapi_cms_id);
      if (strapi_cms_entry) {
        const update_response = await updateCmsEntry(
          row.strapi_cms_id,
          row_core.product_name,
          row_core.price,
          row_core.category,
          row_core.product_image_url
        );
        const sentTimestamp = new Date().toISOString();
        const update = schema.makeUpdateRows(
          "Strapi Items",
          row_core._dataland_key,
          {
            "Sent Timestamp": sentTimestamp,
          }
        );
      }
    } else {
      try {
        // Otherwise, create a new entry, and write back the response CMS ID too
        const create_response = await createCmsEntry(
          row_core.product_name,
          row_core.price,
          row_core.category,
          row_core.product_image_url
        );

        const sentTimestamp = new Date().toISOString();
        const update = schema.makeUpdateRows(
          "Strapi Items",
          row_core._dataland_key,
          {
            "Strapi CMS ID": create_response.id,
            "Sent Timestamp": sentTimestamp,
          }
        );
        mutations.push(update);
      } catch (e) {
        // continue to next
      }
    }
  }

  if (mutations.length === 0) {
    return;
  }

  await runMutations({ mutations });
};

registerTransactionHandler(handler);

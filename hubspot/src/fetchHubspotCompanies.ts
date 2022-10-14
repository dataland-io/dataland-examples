import {
  getCatalogSnapshot,
  getEnv,
  Mutation,
  querySqlSnapshot,
  KeyGenerator,
  OrdinalGenerator,
  registerTransactionHandler,
  runMutations,
  Schema,
  Transaction,
  unpackRows,
} from "@dataland-io/dataland-sdk-worker";

import { isString, isNumber } from "lodash-es";

const hubspot_api_key = getEnv("HUBSPOT_ACCESS_TOKEN");

if (hubspot_api_key == null) {
  throw new Error("Missing environment variable - HUBSPOT_ACCESS_TOKEN");
}

interface HubspotCompany {
  id: number;
  properties: {
    domain: string;
    name: string;
    hs_object_id: number;
    hs_lastmodifieddate: string;
    createdate: string;
  };
  createdAt: string;
  updatedAt: string;
  archived: boolean;
}

const fetchHubspotCompanies = async () => {
  var headers = new Headers();
  headers.append("Authorization", `Bearer ${hubspot_api_key}`);

  let total_counter = 0;
  const full_results = [];

  let url = "https://api.hubapi.com/crm/v3/objects/companies?limit=100";
  let has_next = true;
  do {
    const hubspot_response = await fetch(url, {
      method: "GET",
      headers: headers,
      redirect: "follow",
    });
    const data = await hubspot_response.json();

    if (data.paging == null) {
      has_next = false;
    } else {
      url = data.paging.next.link;
    }

    const results: HubspotCompany[] = data.results;

    const results_processed = results.map(
      ({
        id,
        properties: {
          createdate,
          domain,
          hs_object_id,
          hs_lastmodifieddate,
          name,
        },
        createdAt,
        updatedAt,
        archived,
      }) => ({
        id,
        domain,
        name,
        hs_object_id,
        hs_lastmodifieddate,
        createdate,
        createdAt,
        updatedAt,
        archived,
      })
    );

    if (results_processed) {
      for (const result of results_processed) {
        full_results.push(result);
        total_counter++;
      }
    }
  } while (has_next);

  return full_results;
};

const handler = async (transaction: Transaction) => {
  const { tableDescriptors } = await getCatalogSnapshot({
    logicalTimestamp: transaction.logicalTimestamp,
  });

  const schema = new Schema(tableDescriptors);

  const affectedRows = schema.getAffectedRows(
    "hubspot-companies-trigger",
    "Trigger",
    transaction
  );

  const lookupKeys: number[] = [];
  for (const [key, value] of affectedRows) {
    if (typeof value === "number") {
      lookupKeys.push(key);
      console.log("key noticed: ", key);
    }
  }

  if (lookupKeys.length === 0) {
    console.log("No lookup keys found");
    return;
  }
  const keyList = `(${lookupKeys.join(",")})`;
  console.log("keyList: ", keyList);

  const keyGenerator = new KeyGenerator();
  const ordinalGenerator = new OrdinalGenerator();

  // fetch Hubspot companies from Hubspot
  const hubspotCompanies = await fetchHubspotCompanies();

  if (hubspotCompanies == null) {
    return;
  }

  // fetch existing Hubspot companies
  const existing_hubspot_data = await querySqlSnapshot({
    logicalTimestamp: transaction.logicalTimestamp,
    sqlQuery: `select
      _dataland_key, id
    from "hubspot-companies"`,
  });

  const existing_hubspot_rows = unpackRows(existing_hubspot_data);

  const existing_hubspot_ids = [];
  const existing_hubspot_keys = [];

  for (const existing_hubspot_row of existing_hubspot_rows) {
    existing_hubspot_keys.push(existing_hubspot_row._dataland_key);
    existing_hubspot_ids.push(Number(existing_hubspot_row.id));
  }

  let mutations_batch: Mutation[] = [];
  let batch_counter = 0;
  let batch_size = 100; // push 100 at a time
  let total_counter = 0;

  for (const hubspotCompany of hubspotCompanies) {
    // Generate a new _dataland_key and _dataland_ordinal value
    const id = await keyGenerator.nextKey();
    const ordinal = await ordinalGenerator.nextOrdinal();

    const hubspot_company_id = Number(hubspotCompany.id);

    if (hubspot_company_id == null) {
      continue;
    }

    // check if the Hubspot company already exists
    if (existing_hubspot_ids.includes(hubspot_company_id)) {
      const position = existing_hubspot_ids.indexOf(hubspot_company_id);
      const existing_key = existing_hubspot_keys[position];

      if (!isNumber(existing_key)) {
        continue;
      }

      const update = schema.makeUpdateRows("hubspot-companies", existing_key, {
        id: hubspotCompany.id,
        domain: hubspotCompany.domain,
        name: hubspotCompany.name,
        createdate: hubspotCompany.createdate,
        hs_lastmodifieddate: hubspotCompany.hs_lastmodifieddate,
        createdAt: hubspotCompany.createdAt,
        updatedAt: hubspotCompany.updatedAt,
        archived: hubspotCompany.archived,
      });

      if (update == null) {
        continue;
      }
      mutations_batch.push(update);

      batch_counter++;
      total_counter++;
    } else {
      const insert = schema.makeInsertRows("hubspot-companies", id, {
        _dataland_ordinal: ordinal,
        id: hubspotCompany.id,
        domain: hubspotCompany.domain,
        name: hubspotCompany.name,
        createdate: hubspotCompany.createdate,
        hs_lastmodifieddate: hubspotCompany.hs_lastmodifieddate,
        createdAt: hubspotCompany.createdAt,
        updatedAt: hubspotCompany.updatedAt,
        archived: hubspotCompany.archived,
      });

      if (insert == null) {
        continue;
      }
      mutations_batch.push(insert);

      batch_counter++;
      total_counter++;
    }

    if (batch_counter >= batch_size) {
      await runMutations({ mutations: mutations_batch });
      mutations_batch = [];
      batch_counter = 0;
    } else if (total_counter + batch_size > hubspotCompanies.length) {
      await runMutations({ mutations: mutations_batch });
      mutations_batch = [];
      batch_counter = 0;
    }
  }
};

registerTransactionHandler(handler);

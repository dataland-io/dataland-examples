import {
  getEnv,
  registerCronHandler,
  getDbClient,
  getHistoryClient,
  TableSyncRequest,
} from "@dataland-io/dataland-sdk";

import { tableFromJSON, tableToIPC } from "@apache-arrow/es2015-cjs";

import { isString, isNumber } from "lodash-es";

interface HubspotDeal {
  id: number;
  properties: {
    amount: number;
    closedate: string;
    pipeline: string;
    dealname: string;
    dealstage: string;
    hubspot_owner_id: string;
    hs_object_id: number;
    hs_lastmodifieddate: string;
    createdate: string;
  };
  createdAt: string;
  updatedAt: string;
  archived: boolean;
}

const fetchHubspotOwner = async (
  hubspot_api_key: string,
  hubspot_owner_id: string
) => {
  if (hubspot_owner_id == null) {
    return null;
  }
  const response = await fetch(
    "https://api.hubapi.com/crm/v3/owners/?idProperty=" +
      hubspot_owner_id +
      "&archived=false",
    {
      headers: {
        Authorization: `Bearer ${hubspot_api_key}`,
      },
    }
  );
  const result = await response.json();
  if (result == null) {
    return "";
  } else {
    return (
      result?.results[0]?.firstName +
      " " +
      result?.results[0]?.lastName +
      " (" +
      result?.results[0]?.email +
      ")"
    );
  }
};

const fetchHubspotDeals = async (hubspot_api_key: string) => {
  var headers = new Headers();
  headers.append("Authorization", `Bearer ${hubspot_api_key}`);

  let total_counter = 0;
  const full_results = [];

  const properties = [
    "amount",
    "dealname",
    "dealstage",
    "hubspot_owner_id",
    "closedate",
    "pipeline",
    "createdAt",
    "updatedAt",
  ];

  const properties_string = properties.join(",");
  const properties_string_encoded = encodeURIComponent(properties_string);

  let url =
    "https://api.hubapi.com/crm/v3/objects/deals?limit=100&properties=" +
    properties_string_encoded;
  let has_next = true;
  console.log("fetching Hubspot deals...");

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

    const results: HubspotDeal[] = data.results;

    for (const result of results) {
      const hubspot_owner_id = result.properties.hubspot_owner_id;

      let hubspot_owner = await fetchHubspotOwner(
        hubspot_api_key,
        hubspot_owner_id
      );

      if (hubspot_owner == null) {
        hubspot_owner = "";
      }

      const result_processed = {
        id: result.id,
        amount: result.properties.amount,
        deal_name: result.properties.dealname,
        deal_stage: result.properties.dealstage,
        deal_owner: hubspot_owner,
        close_date: result.properties.closedate,
        pipeline: result.properties.pipeline,
        created_at: result.createdAt,
        updated_at: result.updatedAt,
      };
      full_results.push(result_processed);
      total_counter++;
    }
  } while (has_next);

  console.log("Finished fetching ", full_results.length, " Hubspot deals");
  console.log("preview:", full_results[0]);

  return full_results;
};

const handler = async () => {
  const db = await getDbClient();
  const history = await getHistoryClient();

  const hubspot_api_key = getEnv("HUBSPOT_API_KEY");

  if (hubspot_api_key == null) {
    throw new Error("Missing environment variable - HUBSPOT_API_KEY");
  }

  // fetch Hubspot companies from Hubspot
  const records = await fetchHubspotDeals(hubspot_api_key);

  if (records == null) {
    return;
  }

  const table = tableFromJSON(records);
  const batch = tableToIPC(table);

  const tableSyncRequest: TableSyncRequest = {
    tableName: "hubspot_deals",
    arrowRecordBatches: [batch],
    primaryKeyColumnNames: ["id"],
    dropExtraColumns: false,
    deleteExtraRows: true,
    transactionAnnotations: {},
    tableAnnotations: {},
    columnAnnotations: {},
  };

  await db.tableSync(tableSyncRequest);
};

registerCronHandler(handler);

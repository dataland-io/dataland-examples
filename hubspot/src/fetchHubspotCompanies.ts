import {
  getEnv,
  registerCronHandler,
  getDbClient,
  getHistoryClient,
  TableSyncRequest,
} from "@dataland-io/dataland-sdk";

import { tableFromJSON, tableToIPC } from "@apache-arrow/es2015-cjs";

import { isString, isNumber } from "lodash-es";

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

const fetchHubspotCompanies = async (hubspot_api_key: string) => {
  var headers = new Headers();
  headers.append("Authorization", `Bearer ${hubspot_api_key}`);

  let total_counter = 0;
  const full_results = [];

  let url = "https://api.hubapi.com/crm/v3/objects/companies?limit=100";
  let has_next = true;
  console.log("fetching hubspot companies...");
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
        hs_last_modified_date: hs_lastmodifieddate,
        create_date: createdate,
        created_at: createdAt,
        updated_at: updatedAt,
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

  console.log("Finished fetching ", full_results.length, " Hubspot companies");

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
  const records = await fetchHubspotCompanies(hubspot_api_key);

  if (records == null) {
    return;
  }

  const table = tableFromJSON(records);
  const batch = tableToIPC(table);

  const tableSyncRequest: TableSyncRequest = {
    tableName: "hubspot_companies",
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

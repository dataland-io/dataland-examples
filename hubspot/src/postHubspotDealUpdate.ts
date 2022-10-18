import {
  getEnv,
  getHistoryClient,
  getDbClient,
  MutationsBuilder,
  registerTransactionHandler,
  Transaction,
  unpackRows,
} from "@dataland-io/dataland-sdk";

import { isString, isNumber } from "lodash-es";

const postHubspotUpdate = async (
  hubspot_object_id: string,
  hubspot_close_date: string,
  hubspot_api_key: string
) => {
  const myHeaders = new Headers();
  myHeaders.append("Content-Type", "application/json");
  myHeaders.append("Authorization", `Bearer ${hubspot_api_key}`);

  // first get existing deal
  const url = `https://api.hubapi.com/crm/v3/objects/deals/${hubspot_object_id}`;
  const options = {
    method: "GET",
    headers: myHeaders,
  };

  const existing_deal = await fetch(url, options);

  const existing_deal_json = await existing_deal.json();
  if (existing_deal_json.properties.dealstage === "closedwon") {
    console.log("Update aborted. Deal already closed");
    const status_message = "Error: Deal already closed";
    return {
      status_message: "Update aborted. Deal already closed",
      deal_json: existing_deal_json,
    };
  }

  if (hubspot_close_date == null) {
    console.log("Update aborted. No close date provided.");
    return;
  }

  const raw = JSON.stringify({
    properties: {
      closedate: hubspot_close_date,
    },
  });

  const requestOptions = {
    method: "PATCH",
    headers: myHeaders,
    body: raw,
  };

  const update_deal_response = await fetch(
    "https://api.hubapi.com/crm/v3/objects/deals/9523448933",
    requestOptions
  );

  const result = await update_deal_response.json();

  return {
    status_message: "Success",
    deal_json: result,
  };
};

// TODO: function is defined, now need to call it from a button
const handler = async (transaction: Transaction) => {
  const HUBSPOT_API_KEY = getEnv("HUBSPOT_API_KEY");

  const affected_row_ids = [];

  for (const mutation of transaction.mutations) {
    if (mutation.kind.oneofKind == "updateRows") {
      if (
        mutation.kind.updateRows.columnNames.includes("push_to_hubspot") &&
        mutation.kind.updateRows.tableName === "hubspot_deals"
      ) {
        for (const row of mutation.kind.updateRows.rows) {
          affected_row_ids.push(row.rowId);
        }
      } else {
        return;
      }
    } else {
      return;
    }
  }

  const affected_row_ids_key_list = affected_row_ids.join(",");

  // get all rows where issue_refund was incremented
  const history = await getHistoryClient();
  const response = await history.querySqlMirror({
    sqlQuery: `select _row_id, id, close_date from "hubspot_deals" where _row_id in (${affected_row_ids_key_list})`,
  }).response;

  const rows = unpackRows(response);

  if (rows == null) {
    return;
  }

  for (const row of rows) {
    const id = row.id;
    const close_date = row.close_date;

    if (!isString(id) || !isNumber(row._row_id) || !isString(close_date)) {
      console.log("Error: invalid row");
      continue;
    }

    const response = await postHubspotUpdate(id, close_date, HUBSPOT_API_KEY);

    if (response == null) {
      console.log("Error: No response from Hubspot");
      continue;
    } else {
      const db = await getDbClient();
      await new MutationsBuilder()
        .updateRow("hubspot_deals", row._row_id, {
          processed_at: new Date().toISOString(),
          status_message: response.status_message,
          close_date: response.deal_json.properties.closedate,
        })
        .run(db);
    }
  }
};

registerTransactionHandler(handler);

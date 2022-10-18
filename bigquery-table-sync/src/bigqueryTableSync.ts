import {
  registerCronHandler,
  getEnv,
  getDbClient,
  TableSyncRequest,
} from "@dataland-io/dataland-sdk";

import { createOAuth2Token } from "./gcpAuth";

import { tableFromJSON, tableToIPC } from "@apache-arrow/es2015-esm";

const getOAuth2Token = async () => {
  const serviceAccountKeysJson = getEnv("GCP_SERVICE_ACCOUNT_KEYS_JSON");
  const serviceAccountKeys = JSON.parse(serviceAccountKeysJson);
  const oauth2Token = await createOAuth2Token(
    serviceAccountKeys,
    "https://www.googleapis.com/auth/bigquery"
  );

  return oauth2Token.toString();
};

const fetchFromBigQuery = async () => {
  const myHeaders = new Headers();

  const gcp_access_token = await getOAuth2Token();

  const gcp_project_id = getEnv("GCP_PROJECT_ID");
  if (gcp_project_id == null) {
    throw new Error("Missing environment variable - GCP_PROJECT_ID");
  }

  const gcp_dataset_id = getEnv("GCP_DATASET_ID");
  if (gcp_dataset_id == null) {
    throw new Error("Missing environment variable - GCP_DATASET_ID");
  }

  const gcp_table_id = getEnv("GCP_TABLE_ID");
  if (gcp_table_id == null) {
    throw new Error("Missing environment variable - GCP_TABLE_ID");
  }

  const gcp_api_key = getEnv("GCP_API_KEY");
  if (gcp_api_key == null) {
    throw new Error("Missing environment variable - GCP_API_KEY");
  }

  myHeaders.append("Authorization", gcp_access_token);

  const requestOptions = {
    method: "GET",
    headers: myHeaders,
  };

  const schema_response = await fetch(
    `https://bigquery.googleapis.com/bigquery/v2/projects/${gcp_project_id}/datasets/${gcp_dataset_id}/tables/${gcp_table_id}?key=${gcp_api_key}`,
    requestOptions
  );
  if (schema_response.status != 200) {
    throw new Error(
      `Failed to fetch BigQuery schema: ${await schema_response.text()}`
    );
  }

  const schema_result = await schema_response.json();
  const field_name_array = schema_result.schema.fields.map((field: any) => {
    return field.name;
  });

  const data_response = await fetch(
    "https://bigquery.googleapis.com/bigquery/v2/projects/" +
      gcp_project_id +
      "/datasets/" +
      gcp_dataset_id +
      "/tables/" +
      gcp_table_id +
      "/data?key=" +
      gcp_api_key,
    requestOptions
  );

  const data = await data_response.json();
  const rows = data.rows;
  const formatted_rows = [];
  for (const row of rows) {
    const data = row.f;
    const row_object: any = {};
    for (let i = 0; i < data.length; i++) {
      row_object[field_name_array[i]] = data[i].v;
    }
    formatted_rows.push(row_object);
  }
  return formatted_rows;
};

const handler = async () => {
  const gcp_table_id = getEnv("GCP_TABLE_ID");
  if (gcp_table_id == null) {
    throw new Error("Missing environment variable - GCP_TABLE_ID");
  }

  const gcp_table_identity_column = getEnv("GCP_TABLE_IDENTITY_COLUMN");
  if (gcp_table_identity_column == null) {
    throw new Error("Missing environment variable - GCP_TABLE_IDENTITY_COLUMN");
  }

  const bigquery_records = await fetchFromBigQuery();

  const table = tableFromJSON(bigquery_records);
  const batch = tableToIPC(table);

  const tableSyncRequest: TableSyncRequest = {
    tableName: "bigquery_" + gcp_table_id,
    arrowRecordBatches: [batch],
    primaryKeyColumnNames: [gcp_table_identity_column],
    dropExtraColumns: false,
    deleteExtraRows: true,
    transactionAnnotations: {},
    tableAnnotations: {},
    columnAnnotations: {},
  };

  const db = getDbClient();
  await db.tableSync(tableSyncRequest);

  console.log(
    "Synced " + bigquery_records.length + " records from BigQuery to Dataland"
  );
};

registerCronHandler(handler);

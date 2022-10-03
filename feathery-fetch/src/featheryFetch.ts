import { tableFromJSON, tableToIPC } from "@apache-arrow/es2015-esm";
import {
  registerCronHandler,
  Scalar,
  SyncTable,
  getEnv,
  syncTables,
} from "@dataland-io/dataland-sdk-worker";

const FEATHERY_API_KEY = getEnv("FEATHERY_API_KEY");

if (FEATHERY_API_KEY == null) {
  throw new Error("Missing environment variable - FEATHERY_API_KEY");
}

const FEATHERY_DATALAND_TABLE_NAME = getEnv("FEATHERY_DATALAND_TABLE_NAME");

if (FEATHERY_DATALAND_TABLE_NAME == null) {
  throw new Error(
    "Missing environment variable - FEATHERY_DATALAND_TABLE_NAME"
  );
}

const fetchFeatheryData = async () => {
  const list_users_url = `https://api.feathery.io/api/user/`;

  const options = {
    method: "GET",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Token ${FEATHERY_API_KEY}`,
    },
  };

  const users_response = await fetch(list_users_url, options);
  const users_result = await users_response.json();

  const user_ids = [];

  for (const user of users_result) {
    user_ids.push(user.id);
  }

  const user_data = [];
  for (const user_id of user_ids) {
    const user_url = `https://api.feathery.io/api/field/?id=${user_id}`;
    const user_response = await fetch(user_url, options);
    const user_result = await user_response.json();

    let user_data_obj: { [key: string]: any } = {};

    user_data_obj["User ID"] = user_id;
    for (const field of user_result) {
      console.log("field: ", field);
      console.log("field id: ", field.id);
      console.log("field value: ", field.value);
      user_data_obj[field.id] = field.value;
    }
    user_data.push(user_data_obj);
    console.log("user_data_obj: ", user_data_obj);
  }

  return user_data;
};

const cronHandler = async () => {
  const feathery_data = await fetchFeatheryData();

  console.log("Feathery read successful: ", feathery_data);

  const table = tableFromJSON(feathery_data);
  const batch = tableToIPC(table);

  const syncTable: SyncTable = {
    tableName: FEATHERY_DATALAND_TABLE_NAME,
    arrowRecordBatches: [batch],
    identityColumnNames: ["User ID"],
  };

  await syncTables({
    syncTables: [syncTable],
  });
};

registerCronHandler(cronHandler);

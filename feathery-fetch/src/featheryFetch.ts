import { tableFromJSON, tableToIPC } from "@apache-arrow/es2015-esm";
import {
  registerCronHandler,
  getDbClient,
  TableSyncRequest,
  getEnv,
} from "@dataland-io/dataland-sdk";

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

// check if Feathery Dataland Table Name matches format
if (!FEATHERY_DATALAND_TABLE_NAME.match(/^[a-z0-9_]+$/)) {
  throw new Error(
    "Invalid Feathery Dataland Table Name - must be lowercase alphanumeric and underscores only"
  );
}

const findDuplicates = (arr: Array<String>) => {
  let sorted_arr = arr.slice().sort(); // You can define the comparing function here.
  // JS by default uses a crappy string compare.
  // (we use slice to clone the array so the
  // original array won't be modified)
  let results = [];
  for (let i = 0; i < sorted_arr.length - 1; i++) {
    if (sorted_arr[i + 1] == sorted_arr[i]) {
      results.push(sorted_arr[i]);
    }
  }
  return results;
};

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

    user_data_obj["feathery_user_id"] = user_id;

    const normalized_field_name_list = [];
    for (const field of user_result) {
      const source_field_name = field.id;
      const normalized_field_name = source_field_name
        .toLowerCase()
        .replace(/[\s-]/g, "_")
        .replace(/[^0-9a-z_]/g, "")
        .replace(/^[^a-z]*/, "")
        .slice(0, 63);
      user_data_obj[normalized_field_name] = field.value;
      normalized_field_name_list.push(normalized_field_name);
    }

    const duplicated_normalized_field_names = findDuplicates(
      normalized_field_name_list
    );

    if (duplicated_normalized_field_names.length > 0) {
      throw new Error(
        `Duplicate field names detected - ${duplicated_normalized_field_names} - please rename fields in Feathery`
      );
    }

    user_data.push(user_data_obj);
  }

  return user_data;
};

const cronHandler = async () => {
  const feathery_data = await fetchFeatheryData();

  console.log("Feathery read successful");

  const table = tableFromJSON(feathery_data);
  const batch = tableToIPC(table);

  const tableSyncRequest: TableSyncRequest = {
    tableName: FEATHERY_DATALAND_TABLE_NAME,
    arrowRecordBatches: [batch],
    primaryKeyColumnNames: ["feathery_user_id"],
    dropExtraColumns: false,
    deleteExtraRows: true,
    transactionAnnotations: {},
    tableAnnotations: {},
    columnAnnotations: {},
  };

  const db = getDbClient();
  await db.tableSync(tableSyncRequest);
};

registerCronHandler(cronHandler);

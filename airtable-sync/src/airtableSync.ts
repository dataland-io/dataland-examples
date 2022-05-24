// This is a template that reads all records in an Airtable base, and then upserts them
// into Dataland based on the Airtable record ID.
// Search for the string "TODO" to find the places where you need to change things in this file.

import { isNumber, isString } from "lodash-es";
import {
  getCatalogSnapshot,
  getEnv,
  Mutation,
  KeyGenerator,
  OrdinalGenerator,
  querySqlSnapshot,
  registerTransactionHandler,
  runMutations,
  Schema,
  Transaction,
  unpackRows,
} from "@dataland-io/dataland-sdk-worker";

// TODO: Make sure to declare your Airtable API Key in the .env file and also declare the parameter in spec.yaml.
// See the tutorial for using environment variables here: {link}
const airtable_api_key = getEnv("AIRTABLE_API_KEY");

if (airtable_api_key == null) {
  throw new Error("Missing environment variable - AIRTABLE_API_KEY");
}

// TODO: Update the URL to your Airtable table below:
const airtable_url_base =
  "https://api.airtable.com/v0/apptKL4hgesOoxt0K/Users?view=Grid%20view";

const readFromAirtable = async () => {
  var myHeaders = new Headers();
  myHeaders.append("Content-Type", "application/x-www-form-urlencoded");
  myHeaders.append("Authorization", "Bearer " + airtable_api_key);

  const full_records = [];

  let url = airtable_url_base;
  let offset = "";

  do {
    const airtable_response = await fetch(url, {
      method: "GET",
      headers: myHeaders,
      redirect: "follow",
    });
    const data = await airtable_response.json();
    const records = data.records;

    if (records) {
      for (const record of records) {
        full_records.push(record);
      }
    }

    offset = data.offset;
    url = airtable_url_base + "&offset=" + offset;
  } while (offset);

  return full_records;
};

const handler = async (transaction: Transaction) => {
  const { tableDescriptors } = await getCatalogSnapshot({
    logicalTimestamp: transaction.logicalTimestamp,
  });

  const schema = new Schema(tableDescriptors);

  // TODO: If you're using a different table name than "Airtable Sync Trigger" and different column name than "Trigger sync",
  // update the below arguments. Also make sure this is reflected in spec.yaml as well.
  const affectedRows = schema.getAffectedRows(
    "Airtable Sync Trigger",
    "Trigger sync",
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

  const response = await querySqlSnapshot({
    logicalTimestamp: transaction.logicalTimestamp,
    sqlQuery: `select
      _dataland_key
    from "Airtable Sync Trigger"
    where _dataland_key in ${keyList}`,
  });

  const trigger_rows = unpackRows(response);

  const keyGenerator = new KeyGenerator();
  const ordinalGenerator = new OrdinalGenerator();

  const mutations: Mutation[] = [];
  for (const trigger_row of trigger_rows) {
    const key = Number(trigger_row["_dataland_key"]);
    console.log("key: ", key);

    const update = schema.makeUpdateRows("Airtable Sync Trigger", key, {
      "Last pressed": new Date().toISOString(),
    });

    if (update == null) {
      console.log("No update found");
      continue;
    }
    mutations.push(update);
    console.log("mutations: ", mutations);
  }

  const responseUsersTable = await querySqlSnapshot({
    logicalTimestamp: transaction.logicalTimestamp,
    sqlQuery: `select *
    from "Airtable Users"`,
  });

  const users_rows = unpackRows(responseUsersTable);

  console.log(users_rows);
  let data_id_key_pairs: any = {};

  const existingIds = new Set<string>();
  for (const row of users_rows) {
    const id = row["Record ID"];
    if (!isString(id)) {
      continue;
    }
    existingIds.add(id);
    data_id_key_pairs[id] = row["_dataland_key"];
  }

  // AWU: Update this to keygen
  let syntheticKey = Date.now();
  const airtable_users = await readFromAirtable();

  // AWU: Let's use the "t" library to create a typing from Airtable
  for (const airtable_user of airtable_users) {
    const airtable_user_record_id = airtable_user.id;
    const airtable_user_name = airtable_user.fields["Name"];
    if (!isString(airtable_user_name)) {
      continue;
    }
    const airtable_user_email = airtable_user.fields["Email"];
    const airtable_user_company = airtable_user.fields["Company"];
    const airtable_user_initiate_return =
      airtable_user.fields["Initiate return"];

    const airtable_first_name = airtable_user.fields["First name"];

    let airtable_domain = "";
    let airtable_domain_lowercase = "";

    if (airtable_user_email) {
      airtable_domain = airtable_user_email.substring(
        airtable_user_email.lastIndexOf("@") + 1
      );
      airtable_domain_lowercase = airtable_domain.toLowerCase();
    }

    // if ID exists, overwrite the previous values for the device
    if (existingIds.has(airtable_user_record_id)) {
      // get the synthetic key of the row using defined object before
      const existing_key = data_id_key_pairs[airtable_user_record_id];

      const update = schema.makeUpdateRows("Airtable Users", existing_key, {
        _dataland_ordinal: String(existing_key),
        "Record ID": airtable_user_record_id,
        Name: airtable_user_name,
        Email: airtable_user_email,
        "First Name": airtable_first_name,
        "Initiate Return": airtable_user_initiate_return,
        Company: airtable_user_company,
        Domain: airtable_domain,
      });

      console.log("Updated: ", airtable_user_name, " ", airtable_user_company);
      if (update == null) {
        continue;
      }
      mutations.push(update);
    } else {
      const id = await keyGenerator.nextKey();
      const ordinal = await ordinalGenerator.nextOrdinal();
      const insert = schema.makeInsertRows("Airtable Users", id, {
        _dataland_ordinal: ordinal,
        "Record ID": airtable_user_record_id,
        Name: airtable_user_name,
        Email: airtable_user_email,
        "First Name": airtable_first_name,
        "Initiate Return": airtable_user_initiate_return,
        Company: airtable_user_company,
        Domain: airtable_domain,
      });
      console.log("Inserted: ", airtable_user_name, " ", airtable_user_company);
      if (insert == null) {
        continue;
      }
      mutations.push(insert);
    }
  }
  console.log("mutations:", mutations);
  await runMutations({ mutations });
};

registerTransactionHandler(handler);

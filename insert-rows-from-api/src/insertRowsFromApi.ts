import {
  getCatalogSnapshot,
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

const getRecordsFromJsonPlaceholder = async () => {
  const url = "https://jsonplaceholder.typicode.com/posts";

  const options = {
    method: "GET",
    headers: {
      "Content-Type": "application/json",
    },
  };

  const response = await fetch(url, options);
  const result = await response.json();
  return result;
};

const handler = async (transaction: Transaction) => {
  const { tableDescriptors } = await getCatalogSnapshot({
    logicalTimestamp: transaction.logicalTimestamp,
  });

  const schema = new Schema(tableDescriptors);

  // This function will only get triggered if a user presses a button in a button column named "Trigger",
  // that's part of a table called "Insert Trigger".

  // TODO: If you're using a different name for the trigger table or its column, change it here.
  const affectedRows = schema.getAffectedRows(
    "Insert Trigger",
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

  const response = await querySqlSnapshot({
    logicalTimestamp: transaction.logicalTimestamp,
    sqlQuery: `select
      _dataland_key
    from "Insert Trigger"
    where _dataland_key in ${keyList}`,
  });

  const trigger_rows = unpackRows(response);

  const keyGenerator = new KeyGenerator();
  const ordinalGenerator = new OrdinalGenerator();

  const mutations: Mutation[] = [];
  for (const trigger_row of trigger_rows) {
    const key = Number(trigger_row["_dataland_key"]);
    console.log("key: ", key);

    const update = schema.makeUpdateRows("Insert Trigger", key, {
      "Last pressed": new Date().toISOString(),
    });

    if (update == null) {
      console.log("No update found");
      continue;
    }
    mutations.push(update);
  }

  const json_placeholder_records = await getRecordsFromJsonPlaceholder();

  for (const json_placeholder_record of json_placeholder_records) {
    const id = await keyGenerator.nextKey();
    const ordinal = await ordinalGenerator.nextOrdinal();

    const insert = schema.makeInsertRows("Rows from JSON Placeholder", id, {
      _dataland_ordinal: ordinal,
      "User ID": json_placeholder_record.userId,
      "Post ID": json_placeholder_record.id,
      Title: json_placeholder_record.title,
      Body: json_placeholder_record.body,
    });

    console.log("insert: ", insert);
    if (insert == null) {
      continue;
    }
    mutations.push(insert);
  }
  await runMutations({ mutations });
};

registerTransactionHandler(handler);

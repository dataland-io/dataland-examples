import {
  querySqlMirror,
  registerCronHandler,
  syncTables,
  SyncTable,
  getEnv,
} from "@dataland-io/dataland-sdk-worker";

const sqlQuery = getEnv("SQL_QUERY");
if (sqlQuery == null) {
  throw new Error("Missing environment variable - SQL_QUERY");
}
const materializedViewDatalandTableName = getEnv(
  "MATERIALIZED_VIEW_DATALAND_TABLE_NAME"
);
if (materializedViewDatalandTableName == null) {
  throw new Error(
    "Missing environment variable - MATERIALIZED_VIEW_DATALAND_TABLE_NAME"
  );
}
const materializedViewIdentityColumnName = getEnv(
  "MATERIALIZED_VIEW_IDENTITY_COLUMN_NAME"
);
if (materializedViewIdentityColumnName == null) {
  throw new Error(
    "Missing environment variable - MATERIALIZED_VIEW_IDENTITY_COLUMN_NAME"
  );
}
const keepExtraColumns = getEnv("KEEP_EXTRA_COLUMNS_BOOLEAN");
if (keepExtraColumns == null) {
  throw new Error("Missing environment variable - KEEP_EXTRA_COLUMNS_BOOLEAN");
}

let keepExtraColumnsBoolean = true;

if (keepExtraColumns === "false") {
  keepExtraColumnsBoolean = false;
}

const handler = async () => {
  // Construct the new join query
  const joined_query = await querySqlMirror({
    sqlQuery: `${sqlQuery}`,
  });

  if (joined_query == null) {
    return;
  }

  const syncTable: SyncTable = {
    tableName: `${materializedViewDatalandTableName}`,
    arrowRecordBatches: joined_query.arrowRecordBatches,
    identityColumnNames: [`${materializedViewIdentityColumnName}`],
    keepExtraColumns: keepExtraColumnsBoolean,
  };
  try {
    await syncTables({
      syncTables: [syncTable],
    });
  } catch (e) {
    console.warn(`syncTables failed`, e);
  }
};

registerCronHandler(handler);

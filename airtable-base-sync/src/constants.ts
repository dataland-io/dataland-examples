import { getEnv } from "@dataland-io/dataland-sdk-worker";

console.log("constants test");
export const SYNC_TABLES_MARKER =
  "airtable-table-sync.workers.dataland.io/cron-sync-marker";
export const RECORD_ID = "record-id";

export const AIRTABLE_API_KEY = getEnv("AIRTABLE_API_KEY");
export const AIRTABLE_ALLOW_WRITEBACK_BOOLEAN = getEnv(
  "AIRTABLE_ALLOW_WRITEBACK_BOOLEAN"
);
export const AIRTABLE_BASE_JSON = getEnv("AIRTABLE_BASE_JSON");

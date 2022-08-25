import { getEnv } from "@dataland-io/dataland-sdk-worker";

export const CLIENT_ID = "Id";
export const SYNC_TABLES_MARKER =
  "mindbody.workers.dataland.io/cron-sync-marker";
export const DATALAND_TABLE_NAME = "clients";

export const MINDBODY_API_KEY = getEnv("MINDBODY_API_KEY");
export const ALLOW_WRITEBACK_BOOLEAN = getEnv("ALLOW_WRITEBACK_BOOLEAN");
export const MINDBODY_AUTHORIZATION = getEnv("MINDBODY_AUTHORIZATION");
export const MINDBODY_SITE_ID = getEnv("MINDBODY_SITE_ID");

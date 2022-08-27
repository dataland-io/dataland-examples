import { getEnv } from "@dataland-io/dataland-sdk-worker";

export const MINDBODY_REQUEST_LIMIT = 200;
export const CLIENT_ID = "Id";
export const SYNC_TABLES_MARKER =
  "mindbody.workers.dataland.io/cron-sync-marker";
export const DATALAND_CLIENTS_TABLE_NAME = getEnv(
  "DATALAND_CLIENTS_TABLE_NAME"
);

export const MINDBODY_API_KEY = getEnv("MINDBODY_API_KEY");
export const MINDBODY_AUTHORIZATION = getEnv("MINDBODY_AUTHORIZATION");
export const MINDBODY_SITE_ID = getEnv("MINDBODY_SITE_ID");
export const MINDBODY_ALLOW_WRITEBACK_BOOLEAN = getEnv(
  "MINDBODY_ALLOW_WRITEBACK_BOOLEAN"
);

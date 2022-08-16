import { getEnv } from "@dataland-io/dataland-sdk-worker";

export const RECORD_ID = "record-id";

export const DATALAND_TABLE_NAME = getEnv("DATALAND_TABLE_NAME");

export const AIRTABLE_API_KEY = getEnv("AIRTABLE_API_KEY_5");
export const AIRTABLE_BASE_ID = getEnv("AIRTABLE_BASE_ID_5");
export const AIRTABLE_TABLE_NAME = getEnv("AIRTABLE_TABLE_NAME_5");
export const AIRTABLE_VIEW_NAME = getEnv("AIRTABLE_VIEW_NAME_5");
export const ALLOW_WRITEBACK_BOOLEAN = getEnv("ALLOW_WRITEBACK_BOOLEAN_5");

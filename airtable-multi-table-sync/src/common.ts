import z from "zod";
import { getEnv, wait } from "@dataland-io/dataland-sdk";

export type AirtableImportedValue =
  | undefined
  | string
  | number
  | boolean
  | Record<any, any>
  | any[];
export type AirtableImportedRecord = {
  id: string;
  fields: Record<string, AirtableImportedValue>;
};
export type AirtableImportedRecords = AirtableImportedRecord[];

// NOTE(gab): null: clear cell, undefined: do nothing
export type AirtableUpdateValue = string | number | boolean | undefined;
export type UpdateRecord = {
  id: string;
  fields: Record<string, AirtableUpdateValue>;
};
export type AirtableUpdateRecords = UpdateRecord[];
export type AirtableCreateRecord = {
  fields: Record<string, AirtableUpdateValue>;
};
export type AirtableCreateRecords = AirtableCreateRecord[];
export type AirtableDeleteRecords = string[];

export const RECORD_ID = "record_id";
export const AIRTABLE_FIELD_NAME = "dataland.io/airtable-field-name";

const validateSqlIdentifier = (sqlIdentifier: string): boolean => {
  return /^[a-z][_a-z0-9]{0,62}$/.test(sqlIdentifier);
};

export const validateTableName = (tableName: string): string => {
  if (!validateSqlIdentifier(tableName)) {
    throw new Error(
      `Import - Aborting: Invalid table name: "${tableName}". Must begin with a-z, only contain a-z, 0-9, and _, and have a maximum of 63 characters.`
    );
  }
  return tableName;
};

export const fetchRetry = async (
  fetch: () => Promise<Response>,
  maxTries: number = 4
): Promise<Response | "error"> => {
  for (let tries = 0; tries < maxTries; tries++) {
    if (tries !== 0) {
      await wait(Math.pow(2, tries + 1) * 1000);
    }
    try {
      const response = await fetch();
      if (response.ok) {
        return response;
      }
      console.error(
        `Failed Airtable request - ${response.status}: ${
          response.statusText
        }. Attempt: ${tries + 1} of ${maxTries}`
      );
    } catch (e) {
      console.error(`Network error. Attempt: ${tries + 1} of ${maxTries}`);
    }
  }
  return "error";
};

const syncTargetT = z.object({
  base_id: z.string(),
  table_name: z.string(),
  table_id: z.string(),
  view_id: z.string(),
  read_field_list: z.array(z.string()),
  allowed_writeback_field_list: z.array(z.string()),
});
export const syncTargetsT = z.array(syncTargetT);
export const syncMappingJsonT = z.object({
  sync_targets: syncTargetsT,
});

export interface SyncTarget {
  base_id: string;
  table_name: string;
  table_id: string;
  view_id: string;
  read_field_list: string[];
  allowed_writeback_field_list: Set<string>;
}

export const getSyncTargets = (): SyncTarget[] => {
  const syncMappingJson = getEnv("AIRTABLE_SYNC_MAPPING_JSON");
  let syncMapping;
  try {
    syncMapping = JSON.parse(syncMappingJson);
  } catch (e) {
    console.error(
      `Failed to parse json of AIRTABLE_SYNC_MAPPING_JSON: ${syncMappingJson}`
    );
  }
  const syncTargets = syncMappingJsonT
    .parse(syncMapping)
    .sync_targets.map((target) => {
      validateTableName(target.table_name);
      return {
        ...target,
        allowed_writeback_field_list: new Set(
          target.allowed_writeback_field_list
        ),
      };
    });
  return syncTargets;
};

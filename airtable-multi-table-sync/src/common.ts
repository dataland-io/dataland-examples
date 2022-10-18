import z from "zod";
import { getEnv, wait } from "@dataland-io/dataland-sdk";

export type AirtableImportedValue =
  | undefined
  | string
  | number
  | boolean
  | Record<any, any>
  | any[];
export type AirtableRecord = {
  id: string;
  fields: Record<string, AirtableImportedValue>;
};
export type AirtableRecords = AirtableRecord[];

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

export const validateTableName = (tableName: string): boolean => {
  return validateSqlIdentifier(tableName);
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
  table_id: z.string(),
  view_id: z.string(),
  dataland_table_name: z.string(),
  disallow_insertion: z.boolean().optional(),
  disallow_deletion: z.boolean().optional(),
  omit_fields: z.array(z.string()).optional(),
  read_only_fields: z.array(z.string()).optional(),
});
export const syncTargetsT = z.array(syncTargetT);
export const syncMappingJsonT = z.object({
  sync_targets: syncTargetsT,
});

export interface SyncTarget {
  base_id: string;
  table_id: string;
  view_id: string;
  dataland_table_name: string;
  disallow_insertion?: boolean | undefined;
  disallow_deletion?: boolean | undefined;
  omit_fields?: Set<string> | undefined;
  read_only_fields?: Set<string> | undefined;
}

export const getSyncTargets = (): SyncTarget[] => {
  const syncMappingJson = getEnv("AIRTABLE_SYNC_MAPPING_JSON");
  let syncMapping;
  try {
    syncMapping = syncMappingJsonT.parse(JSON.parse(syncMappingJson));
  } catch (error) {
    throw new Error(
      `Failed to parse json of AIRTABLE_SYNC_MAPPING_JSON: ${syncMappingJson} - ${JSON.stringify(
        error
      )}`
    );
  }

  const syncTargets = syncMapping.sync_targets.map((target) => {
    return {
      ...target,
      read_only_fields:
        target.read_only_fields != null
          ? new Set(target.read_only_fields)
          : undefined,
      omit_fields:
        target.omit_fields != null ? new Set(target.omit_fields) : undefined,
    };
  });
  return syncTargets;
};

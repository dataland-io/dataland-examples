import z from "zod";
import { getEnv, wait } from "@dataland-io/dataland-sdk";

export type AirtableRecordValue =
  | undefined
  | string
  | number
  | boolean
  | Record<any, any>
  | any[];
export type AirtableRecord = {
  id: string;
  fields: Record<string, AirtableRecordValue>;
};
export type AirtableRecords = AirtableRecord[];

// NOTE(gab): null: clear cell, undefined: do nothing
export type AirtableUpdateValue = string | number | boolean | undefined;
export type AirtableUpdateRecord = {
  id: string;
  fields: Record<string, AirtableUpdateValue>;
};
export type AirtableUpdateRecords = AirtableUpdateRecord[];
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

const SyncTarget = z.object({
  base_id: z.string(),
  table_id: z.string(),
  view_id: z.string(),
  dataland_table_name: z.string(),
  disallow_insertion: z.boolean().optional(),
  disallow_deletion: z.boolean().optional(),
  omit_fields: z.array(z.string()).optional(),
  read_only_fields: z.array(z.string()).optional(),
});
export const SyncMappingJson = z.object({
  sync_targets: z.array(SyncTarget),
});

// NOTE(gab): Cannot infer the type of the record from zod,
// since sets are needed for performant lookups in nested loops later.
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
export const getSyncTargets = (): SyncTarget[] | "error" => {
  const syncMappingJson = getEnv("AIRTABLE_SYNC_MAPPING_JSON");
  let syncMappingParsed;
  try {
    syncMappingParsed = JSON.parse(syncMappingJson);
  } catch (e) {
    console.error(
      `Aborting sync: Failed to parse json of AIRTABLE_SYNC_MAPPING_JSON:`,
      e
    );
    return "error";
  }

  const response = SyncMappingJson.safeParse(syncMappingParsed);
  if (response.success !== true) {
    console.error(
      "Aborting sync: AIRTABLE_SYNC_MAPPING_JSON is invalid:",
      response.error.issues
    );
    return "error";
  }

  const syncMapping = response.data;
  const syncTargets: SyncTarget[] = [];
  for (const syncTarget of syncMapping.sync_targets) {
    if (!validateTableName(syncTarget.dataland_table_name)) {
      console.error(
        `Aborting sync: Invalid dataland table name for table: "${syncTarget.dataland_table_name}". Must begin with a-z, only contain a-z, 0-9, and _, and have a maximum of 63 characters.`
      );
      return "error";
    }
    const readOnlyFields =
      syncTarget.read_only_fields != null
        ? new Set(syncTarget.read_only_fields)
        : undefined;
    const omitFields =
      syncTarget.omit_fields != null
        ? new Set(syncTarget.omit_fields)
        : undefined;
    syncTargets.push({
      ...syncTarget,
      read_only_fields: readOnlyFields,
      omit_fields: omitFields,
    });
  }
  return syncTargets;
};
//

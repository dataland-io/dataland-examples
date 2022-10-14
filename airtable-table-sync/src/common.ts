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
  createdTime: string;
  fields: Record<string, AirtableImportedValue>;
};
export type AirtableImportedRecords = AirtableImportedRecord[];

// NOTE(gab): null: clear cell, undefined: do nothing
export type AirtableUpdateValue = string | number | boolean | null | undefined;
export type UpdateRecord = {
  id: string;
  fields: Record<string, AirtableUpdateValue>;
};
export type UpdateRecords = UpdateRecord[];
export type CreateRecord = { fields: Record<string, AirtableUpdateValue> };
export type CreateRecords = CreateRecord[];
export type DeleteRecords = string[];

export const RECORD_ID = "record_id";
export const AIRTABLE_FIELD_NAME = "dataland.io/airtable-field-name";

const validateSqlIdentifier = (sqlIdentifier: string): boolean => {
  return /^[a-z][_a-z0-9]{0,62}$/.test(sqlIdentifier);
};

export const getDatalandTableName = (): string => {
  const datalandTableName = getEnv("AIRTABLE_DATALAND_TABLE_NAME");
  if (!validateSqlIdentifier(datalandTableName)) {
    throw new Error(
      `Import - Aborting: Invalid table name: "${datalandTableName}". Must begin with a-z, only contain a-z, 0-9, and _, and have a maximum of 63 characters.`
    );
  }
  return datalandTableName;
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

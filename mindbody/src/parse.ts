import { Scalar } from "@dataland-io/dataland-sdk-worker";
import { Client, clientT } from "./client";

export const getClientValue = (value: unknown) => {
  // NOTE(gab): our backend returns NaN for empty number fields
  if (typeof value === "number" && isNaN(value)) {
    return null;
  }

  if (typeof value !== "string") {
    return value;
  }

  // TODO(gab): better logic for identifying jsons
  const isObject = value.startsWith("{") && value.endsWith("}");
  const isArray = value.startsWith("[") && value.endsWith("]");

  if (isObject || isArray) {
    try {
      return JSON.parse(value);
    } catch (e) {
      console.error("Writeback - Failed to parse to JSON", { value });
    }
  }
  return value;
};

export const getClient = (row: Record<string, Scalar>) => {
  const clientPost: any = {};
  for (const columnName in row) {
    const value = row[columnName];
    const mboValue = getClientValue(value);

    if (columnName.includes("/~/")) {
      const [parentPropertyKey, propertyKey] = columnName.split("/~/");

      let parentProperty = clientPost[parentPropertyKey];
      if (parentProperty == null) {
        parentProperty = {};
      }
      parentProperty[propertyKey] = mboValue;

      clientPost[parentPropertyKey] = parentProperty;
    } else {
      clientPost[columnName] = mboValue;
    }
  }

  return clientPost;
};

// export const getDatalandWritebackValues = (
//   message: string,
//   client?: Client
// ) => {
//   const values: Record<string, Scalar> = {};
//   if (client != null) {
//     console.log("BEFORE PARSED CLIENT", client);
//     // TODO(gab): handle incorrect data types being returned from  parseClients
//     const parsedClient = parseClients([client])[0]!;
//     console.log("PARSED CLIENT", parsedClient);
//     for (const clientKey in parsedClient) {
//       const clientValue = parsedClient[clientKey];
//       values[clientKey] = clientValue;
//     }
//   }

//   values["MBO push status"] = message;
//   values["MBO pushed at"] = new Date().toISOString();
//   return values;
// };

type ParsedClient = Record<string, Scalar>;

const parseClientValue = (v: any) => {
  if (v == null) {
    return v;
  }

  if (typeof v === "object") {
    return JSON.stringify(v);
  }
  return v;
};

export const parseClients = (clients: Client[]) => {
  const issues: any = [];
  for (const client of clients) {
    const res = clientT.safeParse(client);
    if (res.success === false) {
      for (const issue of res.error.issues) {
        console.error(issue);
        issues.push(issue);
      }
    }
  }
  if (issues.length !== 0) {
    throw new Error(
      "Import - aborting due to incorrect data types being passed"
    );
  }

  const columnNames: Set<string> = new Set();
  for (const client of clients) {
    for (const key in client) {
      const v = client[key as keyof typeof client];

      if (!Array.isArray(v) && typeof v === "object") {
        for (const key2 in v) {
          columnNames.add(`${key}/~/${key2}`);
        }
        continue;
      }

      columnNames.add(key);
    }
  }

  const parsedClients: ParsedClient[] = [];
  for (const client of clients) {
    const parsedClient: ParsedClient = {};
    for (const key in client) {
      const value = client[key as keyof Client];

      if (value == null) {
        // NOTE(gab): these fields only have values for their nested properties.
        // if being null, skip this property
        const NESTED_COLUMNS = [
          "SuspensionInfo",
          "ClientCreditCard",
          "ProspectStage",
          "HomeLocation",
          "Liability",
        ];
        if (NESTED_COLUMNS.includes(key)) {
          continue;
        }
        parsedClient[key] = value;
        continue;
      }

      if (Array.isArray(value)) {
        parsedClient[key] = JSON.stringify(value);
        continue;
      }

      if (typeof value === "object") {
        for (const valueKey in value) {
          const key2 = `${key}/~/${valueKey}`;
          parsedClient[key2] = parseClientValue(
            value[valueKey as keyof typeof value]
          );
        }
        continue;
      }
      parsedClient[key] = parseClientValue(value);
    }

    for (const columnName of columnNames) {
      if (columnName in parsedClient) {
        continue;
      }
      parsedClient[columnName] = null;
    }

    parsedClients.push(parsedClient);
  }
  return parsedClients;
};

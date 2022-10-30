import { z } from "zod";
import {
  Document,
  IntegerValue,
  NullValue,
  StringValue,
  getDocumentId,
} from "../firestore";
import { TypeDefinition } from "./typeDefinition";
import { isNotNull } from "./util";

// TODO: This code references an example Firestore collection named "users"
// You should rename UserDoc, UserRow, UserTypeDef, user_rowToDoc and user_docToRow
// to use the name of your collection instead as the prefix.

// ----------------------------------

// TODO: Define the Firestore document format.
// Create a new line for each field in the Firestore document.
// We use the zod library for schema-validation and security.
export const UserDoc = z.object({
  user_name: z.union([StringValue, NullValue]).optional(),
  email: z.union([StringValue, NullValue]).optional(),
  phone: z.union([StringValue, NullValue]).optional(),
  age: z.union([IntegerValue, NullValue]).optional(),
});

export type UserDoc = z.infer<typeof UserDoc>;

// TODO: Define the Dataland row schema fortmat.
// The "firestore_document_id" column is required.
// Create a new line for each field in the Firestore document.
export const UserRow = z.object({
  firestore_document_id: z.string().nullish(),
  user_name: z.string().nullish(),
  email: z.string().nullish(),
  phone: z.string().nullish(),
  age: z.number().nullish(),
});

export type UserRow = z.infer<typeof UserRow>;

export const user_docToRow = (
  doc: UserDoc,
  fullDocument: Document
): UserRow => {
  // TODO: Convert the Firestore document to a Dataland row.
  // Return an object with the same keys as the Row schema.
  const firestore_document_id = getDocumentId(fullDocument.name);
  const user_name = isNotNull(doc.user_name) ? doc.user_name.stringValue : null;
  const email = isNotNull(doc.email) ? doc.email.stringValue : null;
  const phone = isNotNull(doc.phone) ? doc.phone.stringValue : null;
  const age = isNotNull(doc.age) ? Number(doc.age.integerValue) : null;

  return {
    firestore_document_id,
    user_name,
    email,
    phone,
    age,
  };
};

// TODO: Convert the Dataland row to a Firestore document.
// Return an object with the same keys as the Firestore document.
export const user_rowToDoc = (row: UserRow): UserDoc => {
  const user_name =
    row.user_name != null
      ? { stringValue: row.user_name }
      : { nullValue: null };

  const email =
    row.email != null ? { stringValue: row.email } : { nullValue: null };

  const phone =
    row.phone != null ? { stringValue: row.phone } : { nullValue: null };

  const age =
    row.age != null ? { integerValue: String(row.age) } : { nullValue: null };

  const userDoc: UserDoc = {
    user_name,
    email,
    phone,
    age,
  };

  return userDoc;
};

export const UserTypeDef: TypeDefinition<UserDoc, UserRow> = {
  // TODO: Change the name of the collection in Firestore
  collectionId: "users_example",
  // TODO: Change the name of the synced table in Dataland
  // Must only contain lowercase letters a-z, 0-9, and underscores
  tableName: "users",
  docType: UserDoc,
  rowType: UserRow,
  docToRow: user_docToRow,
  rowToDoc: user_rowToDoc,
};

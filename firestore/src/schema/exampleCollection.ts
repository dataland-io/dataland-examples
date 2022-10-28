import fastJsonStableStringify from "fast-json-stable-stringify";
import { z } from "zod";
import zodToJsonSchema from "zod-to-json-schema";
import { Document, Value, getDocumentId, isMapValue } from "../firestore";
import { TypeDefinition } from "./typeDefinition";
import { flattenValue, unflattenValue } from "./util";

// Assumes there's a simple user document with a name email and phone
export const User = z.object({
  user_name: z.string(),
  email: z.string(),
  phone: z.string(),
});

export const UserJsonSchema = zodToJsonSchema(User);

export const UserDoc = z.record(Value);

export type UserDoc = z.infer<typeof UserDoc>;

export const UserRow = z.object({
  firestore_document_id: z.string().nullish(),
  user: z.string().nullish(),
});

export type UserRow = z.infer<typeof UserRow>;

export const user_docToRow = (
  doc: UserDoc,
  fullDocument: Document
): UserRow => {
  const firestore_document_id = getDocumentId(fullDocument.name);
  const flattened = flattenValue({ mapValue: { fields: doc } });
  const user = fastJsonStableStringify(flattened);
  return {
    firestore_document_id,
    user,
  };
};

export const user_rowToDoc = (row: UserRow): UserDoc => {
  const { user } = row;

  if (user == null) {
    return {};
  }

  const json = JSON.parse(user);
  const unflattened = unflattenValue(json);
  if (!isMapValue(unflattened)) {
    throw new Error("invariant failed - unflattenValue");
  }
  const doc: UserDoc = UserDoc.parse(unflattened.mapValue.fields);
  return doc;
};

export const UserTypeDef: TypeDefinition<UserDoc, UserRow> = {
  collectionId: "user",
  tableName: "user",
  docType: UserDoc,
  rowType: UserRow,
  docToRow: user_docToRow,
  rowToDoc: user_rowToDoc,
};

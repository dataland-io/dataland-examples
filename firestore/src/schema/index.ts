import { UserTypeDef } from "./exampleCollection";
import { TypeDefinition } from "./typeDefinition";

// Table Name => Type Definition
export const TYPE_DEFINITIONS: Record<string, TypeDefinition<any, any>> = {
  exampleCollection: UserTypeDef,
};

export type FirestoreTable = keyof typeof TYPE_DEFINITIONS;

export const DOCUMENT_ID_COLUMN = "firestore_document_id";

export { UserTypeDef };

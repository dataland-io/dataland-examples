import { UserTypeDef } from "./collection_users";
import { GroceryListTypeDef } from "./collection_groceryLists";
import { TypeDefinition } from "./typeDefinition";

// Table Name => Type Definition
export const TYPE_DEFINITIONS: Record<string, TypeDefinition<any, any>> = {
  users: UserTypeDef,
  grocery_lists: GroceryListTypeDef,
};

export type FirestoreTable = keyof typeof TYPE_DEFINITIONS;

export const DOCUMENT_ID_COLUMN = "firestore_document_id";

export { UserTypeDef };

import { UserTypeDef } from "./collection_users";
import { GroceryListTypeDef } from "./collection_groceryLists";
import { TypeDefinition } from "./typeDefinition";

// TODO: Add the type definition you created for your collection below
// The format is <dataland_table_name: TypeDef>
export const TYPE_DEFINITIONS: Record<string, TypeDefinition<any, any>> = {
  users: UserTypeDef,
  grocery_lists: GroceryListTypeDef,
};

export type FirestoreTable = keyof typeof TYPE_DEFINITIONS;

export const DOCUMENT_ID_COLUMN = "firestore_document_id";

export { UserTypeDef };

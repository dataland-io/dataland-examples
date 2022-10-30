import fastJsonStableStringify from "fast-json-stable-stringify";
import { z } from "zod";
import zodToJsonSchema from "zod-to-json-schema";
import { Document, Value, getDocumentId, isMapValue } from "../firestore";
import { TypeDefinition } from "./typeDefinition";
import { TypeHintFn, flattenValue, unflattenValue } from "./util";

export const GroceryList = z.object({
  days: z.array(
    z.object({
      item_name: z.string(),
      quantity: z.number().int(),
    })
  ),
});

export const GroceryListJsonSchema = zodToJsonSchema(GroceryList);

export const GroceryListDoc = z.record(Value);

export type GroceryListDoc = z.infer<typeof GroceryListDoc>;

export const GroceryListRow = z.object({
  firestore_document_id: z.string().nullish(),
  grocery_list: z.string().nullish(),
});

export type GroceryListRow = z.infer<typeof GroceryListRow>;

export const groceryList_docToRow = (
  doc: GroceryListDoc,
  fullDocument: Document
): GroceryListRow => {
  const firestore_document_id = getDocumentId(fullDocument.name);
  const flattened = flattenValue({ mapValue: { fields: doc } });
  const grocery_list = fastJsonStableStringify(flattened);
  return {
    firestore_document_id,
    grocery_list,
  };
};

export const groceryList_rowToDoc = (row: GroceryListRow): GroceryListDoc => {
  const { grocery_list } = row;

  if (grocery_list == null) {
    return {};
  }

  const json = JSON.parse(grocery_list);
  const unflattened = unflattenValue(json);
  if (!isMapValue(unflattened)) {
    throw new Error("invariant failed - unflattenValue");
  }
  const doc: GroceryListDoc = GroceryListDoc.parse(unflattened.mapValue.fields);
  return doc;
};

export const GroceryListTypeDef: TypeDefinition<
  GroceryListDoc,
  GroceryListRow
> = {
  // TODO: Change the name of the collection in Firestore
  collectionId: "grocery_lists_example",
  // TODO: Change the name of the synced table in Dataland
  // Must only contain lowercase letters a-z, 0-9, and underscores
  tableName: "grocery_lists",
  docType: GroceryListDoc,
  rowType: GroceryListRow,
  docToRow: groceryList_docToRow,
  rowToDoc: groceryList_rowToDoc,
};

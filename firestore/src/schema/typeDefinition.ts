import { z } from "zod";
import { Document, Value } from "../firestore";

export type GenericDocType = Record<string, Value | null | undefined>;
export type GenericRowType = {
  firestore_document_id?: string | null | undefined;
};

export interface TypeDefinition<
  DocType extends GenericDocType,
  RowType extends GenericRowType
> {
  collectionId: string;
  tableName: string;
  rowType: z.ZodType<RowType>;
  docType: z.ZodType<DocType>;
  docToRow: (doc: DocType, fullDocument: Document) => RowType;
  rowToDoc: (row: RowType) => DocType;
}

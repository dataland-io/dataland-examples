import { z } from "zod";

export const TableMapping = z.record(z.string());

export type TableMapping = z.infer<typeof TableMapping>;

export const PrimaryKeyColumn = z.object({
  table_name: z.string(),
  column_name: z.string(),
  ordinal_position: z.number(),
});

export type PrimaryKeyColumn = z.infer<typeof PrimaryKeyColumn>;

export const Column = z.object({
  table_name: z.string(),
  column_name: z.string(),
  ordinal_position: z.number(),
  data_type: z.string(),
  is_nullable: z.string(),
});

export type Column = z.infer<typeof Column>;

import z from "zod";

const targetT = z.object({
  base_id: z.string(),
  table_name: z.string(),
  table_id: z.string(),
  view_id: z.string(),
  read_field_list: z.array(z.string()),
  allowed_writeback_field_list: z.array(z.string()),
});
export const syncTargetsT = z.array(targetT);
export const syncMappingJsonT = z.object({
  sync_targets: syncTargetsT,
});

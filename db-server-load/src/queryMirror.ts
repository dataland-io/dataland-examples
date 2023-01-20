import {
  getDbClient,
  unpackRows,
  registerCronHandler,
  getHistoryClient,
} from "@dataland-io/dataland-sdk";

import { loadTable } from "./utils";

const handler = async () => {
  const db = await getDbClient();

  const rows = await loadTable(db, "fruits_mirror");
  for (const row of rows) {
    console.log("test", row);
  }

  const rows_filtered = rows.filter((row) => row.test == "Apple");

  for (const row of rows_filtered) {
    console.log("test FILTERED", row);
  }
};

registerCronHandler(handler);

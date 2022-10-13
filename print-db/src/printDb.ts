import {
  getDbClient,
  getHistoryClient,
  registerTransactionHandler,
  Transaction,
  unpackRows,
} from "@dataland-io/dataland-sdk";

const handler = async (transaction: Transaction) => {
  const db = await getDbClient();
  const history = await getHistoryClient();

  const getCatalogResponse = await db.getCatalog({}).response;
  const table_list = getCatalogResponse?.tableDescriptors.map(
    (table: any) => table.tableName
  );

  console.log("table_list", table_list);

  for (const table of table_list) {
    const queryResponse = await history.querySqlMirror({
      sqlQuery: `SELECT * FROM ${table}`,
    }).response;

    const rows = unpackRows(queryResponse);
    console.log(table);
    console.table(rows);
  }
};

registerTransactionHandler(handler);

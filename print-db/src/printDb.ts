import {
  getCatalogSnapshot,
  querySqlSnapshot,
  registerTransactionHandler,
  Transaction,
  unpackRows,
} from "@dataland-io/dataland-sdk-worker";

const handler = async (transaction: Transaction) => {
  const { tableDescriptors } = await getCatalogSnapshot({
    logicalTimestamp: transaction.logicalTimestamp,
  });

  for (const tableDescriptor of tableDescriptors) {
    const tableName = tableDescriptor.tableName;
    const sqlQuery = `select * from "${tableName}" order by _dataland_ordinal`;
    const response = await querySqlSnapshot({
      logicalTimestamp: transaction.logicalTimestamp,
      sqlQuery,
    });
    const rows = unpackRows(response);
    console.log(tableName);
    console.table(rows);
  }
};

registerTransactionHandler(handler, {
  filterTransactions: "handle-all",
});

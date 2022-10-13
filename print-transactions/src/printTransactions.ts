import {
  Transaction,
  registerTransactionHandler,
} from "@dataland-io/dataland-sdk";

registerTransactionHandler(async (transaction) => {
  console.log("transaction", Transaction.toJsonString(transaction));
});

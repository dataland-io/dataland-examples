import {
  registerTransactionHandler,
  Transaction,
} from "@dataland-io/dataland-sdk-worker";

const handler = async (transaction: Transaction) => {
  console.log("processing transaction", JSON.stringify(transaction));
};

registerTransactionHandler(handler);

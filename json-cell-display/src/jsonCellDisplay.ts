import {
  registerTransactionHandler,
  Transaction,
} from "@dataland-io/dataland-sdk-worker";

const handler = async (transaction: Transaction) => {
  // empty
};

registerTransactionHandler(handler);
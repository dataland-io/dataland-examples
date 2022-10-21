import {
  registerTransactionHandler,
  Transaction,
} from "@dataland-io/dataland-sdk";

const handler = async (transaction: Transaction) => {
  // empty
};

registerTransactionHandler(handler);

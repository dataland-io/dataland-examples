---
to: <%= h.changeCase.param(name) %>/src/<%= h.changeCase.camel(name) %>.ts
---

import { registerTransactionHandler, Transaction } from "@dataland-io/dataland-sdk";

const handler = async (transaction: Transaction) => {
  // empty
};

registerTransactionHandler(handler);

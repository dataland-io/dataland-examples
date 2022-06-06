import {
  getCatalogSnapshot,
  Mutation,
  KeyGenerator,
  OrdinalGenerator,
  registerTransactionHandler,
  runMutations,
  Schema,
  Transaction,
  unpackRows,
} from "@dataland-io/dataland-sdk-worker";

const handler = async (transaction: Transaction) => {
  const { tableDescriptors } = await getCatalogSnapshot({
    logicalTimestamp: transaction.logicalTimestamp,
  });

  const schema = new Schema(tableDescriptors);

  const keyGenerator = new KeyGenerator();
  const ordinalGenerator = new OrdinalGenerator();

  const key = await keyGenerator.nextKey();
  const ordinal = await ordinalGenerator.nextOrdinal();

  const mutations: Mutation[] = [];

  const mutation1 = schema.makeInsertRows("practice", key, {
    _dataland_ordinal: ordinal,
    name: "John",
    email: "jon@gmail.com",
    phone: "123456789",
  });
  mutations.push(mutation1);

  await runMutations({ mutations });
};

registerTransactionHandler(handler);

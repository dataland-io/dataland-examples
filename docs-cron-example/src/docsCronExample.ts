import {
  registerCronHandler,
  CronEvent,
  Mutation,
  KeyGenerator,
  OrdinalGenerator,
  getCatalogMirror,
  Schema,
  runMutations,
} from "@dataland-io/dataland-sdk-worker";

const fetchRandomUserDetails = async () => {
  const url = "https://randomuser.me/api";
  console.log("F url:", url);

  const options = {
    method: "GET",
    headers: {
      "Content-Type": "application/json",
    },
  };

  const response = await fetch(url, options);
  const result = await response.json();
  return result;
};

const handler = async (cron: CronEvent) => {
  console.log(
    `Logging due to cron event with scheduled time: ${cron.scheduledTime}`
  );

  const { tableDescriptors } = await getCatalogMirror();

  const schema = new Schema(tableDescriptors);

  const user = await fetchRandomUserDetails();

  const keyGenerator = new KeyGenerator();
  const ordinalGenerator = new OrdinalGenerator();

  const mutations: Mutation[] = [];

  const id = await keyGenerator.nextKey();
  const ordinal = await ordinalGenerator.nextOrdinal();

  const name = user.results[0].name.first + " " + user.results[0].name.last;
  const email = user.results[0].email;
  const phone = user.results[0].phone;
  const picture = user.results[0].picture.large;

  const insert = schema.makeInsertRows("Random Users", id, {
    _dataland_ordinal: ordinal,
    Name: name,
    Email: email,
    Phone: phone,
    "Picture Link": picture,
  });

  if (insert == null) {
    return;
  }

  mutations.push(insert);

  await runMutations({ mutations });
};

registerCronHandler(handler);

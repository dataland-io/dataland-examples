import {
  registerCronHandler,
  CronEvent,
  MutationsBuilder,
  getDbClient,
} from "@dataland-io/dataland-sdk";

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

  const db = await getDbClient();

  const user = await fetchRandomUserDetails();

  const name = user.results[0].name.first + " " + user.results[0].name.last;
  const email = user.results[0].email;
  const phone = user.results[0].phone;
  const picture = user.results[0].picture.large;

  await new MutationsBuilder()
    .insertRow("random_users", Date.now(), {
      name: name,
      email: email,
      phone: phone,
      picture_link: picture,
    })
    .run(db);
};

registerCronHandler(handler);

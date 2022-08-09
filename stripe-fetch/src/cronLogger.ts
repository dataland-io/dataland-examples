import {
  registerCronHandler,
  CronEvent,
} from "@dataland-io/dataland-sdk-worker";

const handler = async (cron: CronEvent) => {
  console.log(
    `Logging due to cron event with scheduled time: ${cron.scheduledTime}`
  );
};

registerCronHandler(handler);

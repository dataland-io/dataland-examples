import { wait } from "@dataland-io/dataland-sdk";

export const RECORD_ID = "record_id";

export const fetchRetry = async (
  fetch: () => Promise<Response>,
  maxTries: number = 3
): Promise<Response | "error"> => {
  for (let tries = 0; tries < maxTries; tries++) {
    if (tries !== 0) {
      await wait(Math.pow(2, tries + 1) * 1000);
    }

    try {
      const response = await fetch();
      if (response.ok) {
        return response;
      }
      console.error(
        `Failed Airtable request: ${response.status}: ${
          response.statusText
        }. Attempt: ${tries + 1} of ${maxTries}`
      );
    } catch (e) {
      console.error(`Network error. Attempt: ${tries + 1} of ${maxTries}`);
    }
  }
  return "error";
};

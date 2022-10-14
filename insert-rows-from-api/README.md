# Insert rows into Dataland from API response

> As of Oct 12, 2022 - this module uses unsupported Dataland APIs.

This example walks through how you can issue a GET request with a Dataland worker, and insert each response item as a new row in Dataland. We'll use the JSON Placeholder API for this example.

Here's a Loom video of how it works after `dataland deploy`:
https://www.loom.com/share/5756b43533ca48e59f86918c10cb24ac

## Setup instructions

1. To run this example, clone this repo . Then, from the `insert-rows-from-api/` folder, run:

```sh
npm install
dataland deploy
```

2. Authenticate to your workspace with `dataland config init` and your access key. You can get an access key by navigating to Settings > Access keys in the Dataland app.
3. Run `dataland deploy`, then `dataland tail` to start streaming logs.
4. Go to the Dataland app, add a row to the `Insert Trigger` table, and click the `Trigger` button.
5. You'll see 100 rows appear in the `Rows from JSON Placeholder` table. It was populated by the 100 items in the response from the JSON Placeholder API.

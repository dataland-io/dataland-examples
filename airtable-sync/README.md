# Airtable Sync Example

You can sync any Airtable table into Dataland with this worker. This example repo consist of:

- A worker that takes an Airtable table (via URL), and imports each record in the table as a row in the imported records table
- A "trigger" table that has a button column. Clicking this button triggers the worker's import operation. In the future, you can also trigger this sync to occur on a schedule without needing manual input.
- A table that stores the imported records

Here's how it looks in practice:

![overview gif](https://i.ibb.co/1M0Qg3R/Import-Airtable.gif)

To run this example, clone this repo , and from the `airtable-sync/` folder, run:

```sh
npm install
dataland deploy
```

To customize this to import your own Airtable table:

1. Follow each of the "TODO"s in the `airtableSync.ts` file
2. Follow each of the "TODO"s in the `spec.yaml` file.
3. Authenticate to your workspace with `dataland config init` and your access key. You can get an access key by navigating to Settings > Access keys.
4. Run `dataland deploy`, then `dataland tail` to start streaming logs.
5. Go to the Dataland app, add a row to the Airtable Sync Trigger table, and click the Trigger button to kick off the import process
6. Rows of data will start to appear in your Records from Airtable table.

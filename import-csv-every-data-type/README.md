# Import CSV

You can import any CSV into Dataland with this worker. This example repo consist of:

- A worker that takes a plaintext CSV file (via URL), and imports each record as a row in the imported records table
- A "trigger" table that has a button column. Clicking this button triggers the worker's import operation.
- A table that stores the imported records

Here's how it looks in practice:

![overview gif](import-csv.gif)

To run this example, clone this repo , and from the `import-csv/` folder, run:

```sh
npm install
dataland deploy
```

To customize this to import your own CSV:

1. Follow each of the "TODO"s in the `importCsv.ts` file
2. Follow each of the "TODO"s in the `spec.yaml` file.
3. Authenticate to your workspace with `dataland config init` and your access key. You can get an access key by navigating to Settings > Access keys.
4. Run `dataland deploy`, then `dataland tail` to start streaming logs.
5. Go to the Dataland app, add a row to the import_trigger table, and click the Trigger button to kick off the import process
6. Rows of data will start to appear in your records_from_csv table.

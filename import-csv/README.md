# Import CSV

You can import any CSV into Dataland with this worker. This example repo consist of:

- A worker that takes a plaintext CSV file (via URL), and imports each record as a row in the imported records table
- A "trigger" table that has a button column. Clicking this button triggers the worker's import operation.
- A table that stores the imported records

To run this example, clone this repo , and from the `import-csv/` folder, run:

```sh
npm install
dataland deploy
```

To customize this to import your own CSV:

1. Follow each of the "TODO"s in the `importCsv.ts` file
2. Authenticate to your workspace with `dataland config init` and your access key. You can get an access key by navigating to Settings > Access keys.
3. Run `dataland deploy`, then `dataland tail` to start streaming logs.
4. Rows of data will start to appear in your records_from_csv table.

# Import CSV

You can import any CSV into Dataland with this worker. This example repo consist of:

- A worker that takes a plaintext CSV file (via URL), and imports each record as a row in the imported records table
- A "trigger" table that has a button column. Clicking this button triggers the worker's import operation.
- A table that stores the imported records

Here's how it looks in practice:

![overview gif](https://i.ibb.co/S3w3vxZ/csv-import-overview.gif)

To run this example, clone this repo , and from the `import-csv/` folder, run:

```sh
npm install
dataland deploy
```

To customize this to import your own CSV:

1. Follow each of the "TODO"s in the `importCsv.ts` file
2. Follow each of the "TODO"s in the `spec.yaml` file.
3. Run dataland deploy

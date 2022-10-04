# Overview

Set up a two-way sync for your Airtable table in Dataland.

If the `AIRTABLE_ALLOW_WRITEBACK_BOOLEAN` is set to `true`, any changes done in the Dataland UI will execute a transaction that writes back to your Airtable.

This includes:

- Creating new records
- Updating existing record fields, except for computed columns\*. See details below.
- Deleting records

Dataland always treats your Airtable instance as the source of truth. Any invalid transactions attempted from Dataland will be rejected by your source Airtable, and then overriden in Dataland by the next sync from Airtable. For example, since the Airtable API prevents updates to formula column values, any change from Dataland to a formula column will be rejected.

Data in the Dataland UI will be re-updated every 5 minutes by default. This cadence can be configurable.

## Parameter setup

| Name                               | About                                                                                                                                             |
| ---------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| `airtable-api-key`                 | Your API key. This can take a service user's API key to lock down specific permissions on the Airtable side.                                      |
| `airtable-base-id`                 | A base's ID                                                                                                                                       |
| `airtable-table-name`              | A table's ID or display name                                                                                                                      |
| `airtable-view-name`               | A view's ID or display name                                                                                                                       |
| `dataland-table-name`              | Dataland will create a table with this name, and replicate Airtable data into it                                                                  |
| `airtable-allow-writeback-boolean` | If `true`, row updates/creation/deletion in Dataland will attempt writebacks to your Airtable table. If `false`, no writeback is attempted.       |
| `airtable-fields-list`             | A comma separated string of the subset of fields you'd like to sync from an Airtable view. Case-sensitive. To use all fields, simply write `ALL`. |

### How to get the Airtable IDs for base, table, and view

Go to your Airtable table in the browser. If using the Airtabledesktop app, press `Ctrl` + `L` on Windows or `⌘` + `L` on Mac to get the link.

You can then parse out the base ID, table ID, and view ID like shown:

`https://airtable.com/{{base-id}}/{{table-id}}/{{view-id}}?blocks=hide`

For example, for the URL `https://airtable.com/appIY466wioivh2aU/tblXoP3z4g038nDsu/viwtQsYbKqlW9EfOZ?blocks=hide`:

- Base ID: `appIY466wioivh2aU`
- Table ID: `tblXoP3z4g038nDsu`
- View ID: `viwtQsYbKqlW9EfOZ`

## Field types that reject writeback

The Dataland table comes with a column titled `record-id`, which maps a row in Dataland to a record in Airtable. `record-id` can't be edited.

In addition to `record-id`, there are computed fields that reject any updates via API, and will therefore reject updates from Dataland. These computed field types include:

- Autonumber
- Button
- Count
- Created by
- Created time
- Formula
- Last modified by
- Last modified time
- Lookup
- Rollup

Dataland also doesn't support writeback for these field types:

- Attachment
- Barcode
- Collaborator

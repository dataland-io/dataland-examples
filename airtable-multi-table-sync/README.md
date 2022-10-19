# Overview

Set up a two-way sync for multiple Airtable tables and Dataland.

This can also be a read-only sync; read on below for details.

This includes:

- Creating new records
- Updating existing record fields, except for computed columns\*. See details below.
- Deleting records

Dataland always treats your Airtable instance as the source of truth. Any invalid transactions attempted from Dataland will be rejected by your source Airtable, and then overriden in Dataland by the next sync from Airtable. For example, since the Airtable API prevents updates to formula column values, any change from Dataland to a formula column will be rejected, and then updated to show the value from Airtable at the next sync.

Data in the Dataland UI will be synced every 5 minutes by default. This cadence is configurable.

To make a "read-only" sync, set the value of `AIRTABLE_ALLOW_WRITEBACK_BOOLEAN` to `false`.

## Video walkthrough

This [video](https://www.loom.com/share/4e5bea9c6a6343d68cb2c23851c48cb2) walks through the installation setup fully.

## Parameter setup

| Name                               | About                                                                                                                                       |
| ---------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| `airtable-api-key`                 | Your API key. This can take a service user's API key to lock down specific permissions on the Airtable side.                                |
| `airtable-allow-writeback-boolean` | If `true`, row updates/creation/deletion in Dataland will attempt writebacks to your Airtable table. If `false`, no writeback is attempted. |
| `airtable-sync-mapping-json`       | A stringified JSON of a specific format - see below for details.                                                                            |

## Parameter explanation

The following is an example `AIRTABLE_SYNC_MAPPING_JSON`:

```json
{
  "sync_targets": [
    {
      "base_id": "appB5C0P7ihcpAkkO",
      "table_id": "tblbXqDJIHroaitOP"
      "view_id": "viwIbjQaNkNZUZwXC",
      "dataland_table_name": "airtable_support_tickets",
      "omit_fields": ["Last name"],
      "read_only_fields": ["Order Total"]
    },
    {
      "base_id": "appB5C0P7ihcpAkkO",
      "table_id": "tblNKt1iT5OFf7IjR",
      "view_id": "viwfCixmEdpyGJ621",
      "dataland_table_name": "airtable_orders",
      "omit_fields": [],
      "read_only_fields": [],
      "disallow_insertion": true,
      "disallow_deletion": false
    }
  ]
}
```

You'll then need to provide a stringified, one-line JSON to Dataland. To generate this easily, you can plug in your JSON [on this CodeSandbox snippet](https://codesandbox.io/s/js-playground-forked-yzo9ot?file=/src/index.js). Click on `Console` in the bottom right, and copy the stringified JSON. Then paste the stringified JSON in the `airtable-sync-mapping-json` input in the Dataland module installation page.

If deploying with `dataland deploy`, then use the same stringified JSON, but wrap it with single quotes when using in your .env variable, i.e.:

```
DL_PARAM_AIRTABLE_SYNC_MAPPING_JSON='{"sync_targets":[{"base_id":"appg1sbxvYR6VEi9l","table_id":"tblMSO2Yt1Sl88Ckx","view_id":"viww84Z7xN9sHXQb1","dataland_table_name":"testing","omit_fields":["Last name"],"read_only_fields":["Digits!!"]},{"base_id":"appg1sbxvYR6VEi9l","table_id":"tblSyrMVgV8b3tfW2","view_id":"viwCOHJ4kHpiCitNw","dataland_table_name":"no_insert","omit_fields":[],"read_only_fields":[],"disallow_insertion":true},{"base_id":"appg1sbxvYR6VEi9l","table_id":"tblImgvlnwlzO6CZI","view_id":"viwsCwsuriCGnVQQc","dataland_table_name":"no_deletion","omit_fields":["Comment"],"read_only_fields":[],"disallow_deletion":true}]}'
DL_PARAM_AIRTABLE_API_KEY={{YOUR-API-KEY}}
DL_PARAM_AIRTABLE_ALLOW_WRITEBACK_BOOLEAN="true"
```

### How to get the Airtable IDs for base, table, and view

Go to your Airtable table in the browser. If using the Airtable desktop app, press `Ctrl` + `L` on Windows or `⌘` + `L` on Mac to get the link.

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

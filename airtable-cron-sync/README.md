# Overview

Sync your data from Airtable into Dataland.

If the `ALLOW_WRITEBACK_FLAG` is enabled, any changes done in the Dataland UI will execute a transaction that writes back to your Airtable.

This includes:

- Creating new records
- Updating existing record fields, except for "computed columns"\*. See details below.
- Deleting records

Dataland always treats your Airtable instance as the source of truth. Any invalid transactions attempted from Dataland will be rejected by your source Airtable. For example, since the Airtable API prevents updates to formula column values, any change from Dataland to a formula column will be rejected.

Data in the Dataland UI will be re-updated every 15 minutes by default. This cadence can be configurable.

## Parameter setup

This module contains several workers for parameters:

| Name                      | About                                                                                                                                       |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| `airtable-api-key`        | Your API key. This can take a service user's API key to lock down specific permissions on the Airtable side.                                |
| `airtable-base-id`        | A base's ID                                                                                                                                 |
| `airtable-table-name`     | A table's ID or display name                                                                                                                |
| `airtable-view-name`      | A view's ID or display name                                                                                                                 |
| `allow-writeback-boolean` | If `true`, row updates/creation/deletion in Dataland will attempt writebacks to your Airtable table. If `false`, no writeback is attempted. |

| `dataland-table-name` | Dataland will create a table with this name, and replicate Airtable data into it |

### How to get your Airtable base, table, or view IDs

## Computed columns that reject updates

Airtable has certain fields that reject any updates via API, and will therefore reject updates from Dataland:

- Autonumber
- Button
- Count
- Created time, Created By, Modified time, Modified by
- Formula
- Lookup
- Rollup

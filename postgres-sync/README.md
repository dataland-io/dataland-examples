# Overview

Use Dataland as an admin GUI into your Postgres data. This module discovers and replicates all of your data from your Postgres into Dataland on a recurring 15 minute cadence.

Any changes done in the Dataland UI will execute a transaction that writes back to your Postgres. Dataland always treats your Postgres instance as the source of truth. Any invalid transactions attempted from Dataland will be rejected by your source Postgres.

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
| `dataland-table-name`     | Dataland will create a table with this name, and replicate Airtable data into it                                                            |

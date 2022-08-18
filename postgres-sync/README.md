# Overview

Use Dataland as an admin GUI into your Postgres data. This module discovers and replicates all of your data from your Postgres into Dataland on a recurring 15 minute cadence.

Any changes done in the Dataland UI will execute a transaction that writes back to your Postgres. Dataland always treats your Postgres instance as the source of truth. Any invalid transactions attempted from Dataland will be rejected by your source Postgres.

Data in the Dataland UI will be re-updated every 15 minutes by default. This cadence can be configurable.

## Parameter setup

| Name                        | About                                                                                                                                                                                                                      |
| --------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `pghost`                    | Database host                                                                                                                                                                                                              |
| `pgport`                    | Database port                                                                                                                                                                                                              |
| `pgdatabase`                | Database name                                                                                                                                                                                                              |
| `pguser`                    | Database user                                                                                                                                                                                                              |
| `pgpassword`                | Database password                                                                                                                                                                                                          |
| `pgschema`                  | Database schema                                                                                                                                                                                                            |
| `allow-drop-tables-boolean` | If `true`, Dataland executes DROP TABLE commands if tables are deleted. Otherwise if `false` (or any other value), Dataland ignores any DROP TABLE command.                                                                |
| `allow-writeback-boolean`   | If `true`, row updates/creation/deletion in Dataland will attempt the same transactions in the source Postgres. Otherwise if `false` (or any other value), Dataland is just a read-only interface to your source Postgres. |

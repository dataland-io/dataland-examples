# Overview

Use Dataland as an admin GUI into your Postgres data. This module discovers and replicates all of your data from your Postgres into Dataland on a recurring every 30 seconds cadence.

Any changes done in the Dataland UI will execute a transaction that writes back to your Postgres. Dataland always treats your Postgres instance as the source of truth. Any invalid transactions attempted from Dataland will be rejected by your source Postgres.

Data in the Dataland UI will be re-updated every 30 seconds by default. This cadence can be configurable.

## Parameter setup

| Name                  | About                                                                                                                                                                                                                      |
| --------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `pg-host`             | Database host                                                                                                                                                                                                              |
| `pg-port`             | Database port                                                                                                                                                                                                              |
| `pg-database`         | Database name                                                                                                                                                                                                              |
| `pg-user`             | Database user                                                                                                                                                                                                              |
| `pg-password`         | Database password                                                                                                                                                                                                          |
| `pg-schema`           | Database schema                                                                                                                                                                                                            |
| `mysql-table-mapping` | The list of synced tables from MySQL. See below for format details.                                                                                                                                                        |
| `pg-do-writeback`     | If `true`, row updates/creation/deletion in Dataland will attempt the same transactions in the source Postgres. Otherwise if `false` (or any other value), Dataland is just a read-only interface to your source Postgres. |

### Inputting `pg-table-mapping`

This parameter is a stringified JSON that specifies the mapping between the synced tables from Postgres and the Dataland tables they will sync into.

The JSON follows the format:

```json
{
  "pg_table_name_1": "dataland_table_name_1",
  "pg_table_name_2": "dataland_table_name_2"
  // and so on
}
```

For example, let's say we want to sync three tables from MySQL database into Dataland. In MySQL, the desired tables are titled `Customers`, `Orders`, and `Products`. We want to sync them into Dataland with the names `synced_customers`, `synced_orders`, and `synced_products` respectively. The resulting JSON would be:

```json
{
  "Customers": "synced_customers",
  "Orders": "synced_orders",
  "Products": "synced_products"
}
```

The stringified version of this JSON can then used as the `pg-table-mapping` module installation form:

```
'{"Customers":"synced_customers","Orders":"synced_orders","Products":"synced_products"}'
```

Or, in the .env file:

```env
DL_PARAM_PG_TABLE_MAPPING='{"Customers":"synced_customers","Orders":"synced_orders","Products":"synced_products"}'
```

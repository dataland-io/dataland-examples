# Overview

Use Dataland as an admin GUI into your MySQL data. This module discovers and replicates all of your data from your MySQL into Dataland on a recurring 15 minute cadence.

Any changes done in the Dataland UI will execute a transaction that writes back to your MySQL. Dataland always treats your MySQL instance as the source of truth. Any invalid transactions attempted from Dataland will be rejected by your source MySQL.

Data in the Dataland UI will be re-updated every 15 minutes by default. This cadence can be configurable.

## Parameter setup

| Name                  | About                                                               |
| --------------------- | ------------------------------------------------------------------- |
| `mysql-host`          | Database host                                                       |
| `mysql-port`          | Database port                                                       |
| `mysql-db`            | Database name                                                       |
| `mysql-user`          | Database user                                                       |
| `mysql-password`      | Database password                                                   |
| `mysql-table-mapping` | The list of synced tables from MySQL. See below for format details. |

### Inputting `mysql-table-mapping`

This parameter is a stringified JSON that specifies the mapping between the synced tables from MySQL and the Dataland tables they will sync into.

The JSON follows the format:

```json
{
  "mysql-table-name-1": "dataland-table-name-1",
  "mysql-table-name-2": "dataland-table-name-2"
  // and so on
}
```

For example, let's say we want to sync three tables from MySQL database into Dataland. In MySQL, the desired tables are titled `Customers`, `Orders`, and `Products`. We want to sync them into Dataland with the names `Synced Customers`, `Synced Orders`, and `Synced Products` respectively. The resulting JSON would be:

```json
{
  "Customers": "Synced Customers",
  "Orders": "Synced Orders",
  "Products": "Synced Products"
}
```

The stringified version of this JSON can then used as the `mysql-table-mapping` module installation form:

```
'{"Customers": "Synced Customers","Orders": "Synced Orders","Products": "Synced Products"}'
```

Or, in the .env file:

```env
DL_PARAM_MYSQL_TABLE_MAPPING='{"Customers": "Synced Customers","Orders": "Synced Orders","Products": "Synced Products"}'
```

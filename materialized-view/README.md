# Overview

Define a custom SQL query that generates a materialized view as an output table every 30 seconds. This module lets you construct joins between your other tables in your workspace.

It's helpful to test SQL queries in the Dataland CLI with the command `dataland sql`, before using the query in this module.

`SELECT` statements are all allowed - even complex queries. See [the docs](https://docs.dataland.io/guides/querying-workspace-in-sql.html) for more detail.

## Parameter setup

| Name                                     | About                                                                                                                                       |
| ---------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| `sql-query`                              | SQL query                                                                                                                                   |
| `materialized-view-dataland-table-name`  | Output table name in Dataland                                                                                                               |
| `materialized-view-identity-column-name` | Primary key column for the join. Composite keys can be used as well, but must be specified as `"{column1}", "{column2}", "{column3}"`, etc. |
| `keep-extra-columns-boolean`             | This preserves any extra columns added to the output table in the Dataland UI.                                                              |

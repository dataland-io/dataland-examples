# Overview

Define a custom SQL query that generates a materialized view as an output table. This table gets updated every 30 seconds. This module lets you construct joins between your other tables in your workspace.

It's helpful to test SQL queries in the Dataland CLI with the command `dataland sql`, before using the query in this module. Note that while the CLI may render columns with non-unique names, Dataland will log an `internal error` if the statement output has columns with non-unique names. You can run the command `dataland tail` to identify this issue.

`SELECT` statements are all allowed - even complex queries. See [the docs](https://docs.dataland.io/guides/querying-workspace-in-sql.html) for more detail.

## Parameter setup

| Name                                | About                                                                                                                                       |
| ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| `sql-query`                         | SQL query                                                                                                                                   |
| `sql-view-dataland-table-name`      | Output table name in Dataland                                                                                                               |
| `sql-view-primary-key-column-name`  | Primary key column for the join. Composite keys can be used as well, but must be specified as `"{column1}", "{column2}", "{column3}"`, etc. |
| `example-input-dataland-table-name` | Write one of the names of the input tables for the join                                                                                     |

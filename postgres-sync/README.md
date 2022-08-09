# Overview

Use Dataland as an admin GUI into your Postgres data. This module discovers and replicates all of your data from your Postgres into Dataland on a recurring 15 minute cadence.

Any changes done in the Dataland UI will execute a transaction that writes back to your Postgres. Dataland always treats your Postgres instance as the source of truth. Any invalid transactions attempted from Dataland will be rejected by your source Postgres.

Data in the Dataland UI will be re-updated every 15 minutes by default. This cadence can be configurable.

# Overview

Use Dataland as an admin panel into your Hubspot data. See all key Hubspot objects as Dataland tables, and take actions like changing Hubspot deal close dates.

### Tables

This module replicates the following objects from Hubspot into Dataland as tables:

| Tables            | Sync schedule |
| ----------------- | ------------- |
| hubspot_companies | Every 5 mins  |
| hubspot_contacts  | Every 5 mins  |
| hubspot_deals     | Every 5 mins  |

### Actions

This module contains several workers for actions:

| Purpose                     | Worker file                | How to trigger                                 |
| --------------------------- | -------------------------- | ---------------------------------------------- |
| Push update to Hubspot Deal | `postHubspotDealUpdate.ts` | Click `Update Hubspot` button on hubspot_deals |

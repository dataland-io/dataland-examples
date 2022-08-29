# Overview

Use Dataland as an admin panel into Orangetheory's members and leads, synced from Mindbody.

See views like:

- Active members who haven't signed their liability release
- Prospective members sorted by their first appointment date
- Manage member notification preferences

Update client information like:

- Prospect stage and member status
- Contact information or preferences
- Liability release
- Credit card
- Home location

## Tables

This module replicates the following objects from Mindbody into Dataland as tables:

| Tables  | Sync schedule | Mindbody Documentation                                                                   |
| ------- | ------------- | ---------------------------------------------------------------------------------------- |
| clients | Every 15 mins | [Clients](https://developers.mindbodyonline.com/PublicDocumentation/V6#client-endpoints) |

## Parameter setup

| Name                               | About                                                                                                                        |
| ---------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| `mindbody-api-key`                 | A Mindbody API key                                                                                                           |
| `mindbody-authorization`           | A Mindbody staff authorization token                                                                                         |
| `mindbody-site-id`                 | A Mindbody site ID                                                                                                           |
| `dataland-clients-table-name`      | The name of the output table in Dataland                                                                                     |
| `mindbody-allow-writeback-boolean` | If `true`, row updates in Dataland will attempt writebacks to your Mindbody instance. If `false`, no writeback is attempted. |

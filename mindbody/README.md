# Overview

Use Dataland as an admin panel into Orangetheory's members and leads, synced from Mindbody.

See views like:

- Clients with the lowest account balance by location
- Prospective members who haven't signed a liability release

Update client information like:

- Prospect stage and member status
- Contact information or preferences
- Liability release
- Credit card
- Home location

## Tables

This module replicates the following objects from Mindbody into Dataland as tables:

| Tables  | Sync schedule | Mindbody Documentation                                    |
| ------- | ------------- | --------------------------------------------------------- |
| clients | Every 15 mins | [Customers](https://stripe.com/docs/api/customers/object) |

## Parameter setup

| Name                          | About                                    |
| ----------------------------- | ---------------------------------------- |
| `mindbody-api-key`            | A Mindbody API key                       |
| `mindbody-authorization`      | A Mindbody staff authorization token     |
| `mindbody-site-id`            | A Mindbody site ID                       |
| `dataland-clients-table-name` | The name of the output table in Dataland |
| `allow-writeback-boolean`     |                                          |

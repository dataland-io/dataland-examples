# Overview

Set up a read-only sync for your Smartsheet table in Dataland.

Data in the Dataland UI will be re-updated every 5 minutes by default. This cadence can be configurable.

## Parameter setup

| Name                             | About                                                                                                          |
| -------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| `smartsheet-api-key`             | Your API key. This can take a service user's API key to lock down specific permissions on the Smartsheet side. |
| `smartsheet-sheet-id`            | A sheet's ID                                                                                                   |
| `smartsheet-dataland-table-name` | The name of the output table in Dataland                                                                       |

### How to get your Smartsheet sheet ID

Navigate to your Smartsheet table in the browser. You can then parse out the sheet ID as shown:

`https://app.smartsheet.com/sheets/{sheetId}}?view=grid`

For example, for the URL `https://app.smartsheet.com/sheets/q4Gv2cwPHMcVHPqFVFxChrM9qCpFFvHGXHHW8591?view=grid`, the sheet ID is `q4Gv2cwPHMcVHPqFVFxChrM9qCpFFvHGXHHW8591`.

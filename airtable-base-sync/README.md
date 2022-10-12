# Overview

Set up a two-way sync for multiple Airtable tables and Dataland.

## Parameter explanation

The parameter `AIRTABLE_SYNC_MAPPING_JSON` has a JSON format like so:

```json
{
  "sync_targets": [
    {
      "base_id": "appB5C0P7ihcpAkkO",
      "table_name": "support_tickets",
      "table_id": "tblbXqDJIHroaitOP",
      "view_id": "viwIbjQaNkNZUZwXC",
      "read_field_list": [
        "support_ticket_id",
        "ticket_message",
        "ticket_category",
        "order_id"
      ],
      "allowed_writeback_field_list": ["ticket_message", "order_id"]
    },
    {
      "base_id": "appB5C0P7ihcpAkkO",
      "table_name": "orders",
      "table_id": "tblNKt1iT5OFf7IjR",
      "view_id": "viwfCixmEdpyGJ621",
      "read_field_list": ["order_id", "order_total", "order_date"],
      "allowed_writeback_field_list": []
    }
  ]
}
```

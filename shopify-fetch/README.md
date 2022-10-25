# Overview

Use Dataland to sync in all of your Shopify data. See all key Shopify objects as Dataland tables.

### Tables

This module replicates the following objects from Shopify into Dataland as tables:

| Tables                   | Sync schedule | Shopify object                                                                       |
| ------------------------ | ------------- | ------------------------------------------------------------------------------------ |
| shopify_products         | Every minute  | [Products](https://shopify.dev/api/admin-rest/2022-10/resources/product)             |
| shopify_orders           | Every minute  | [Orders](https://shopify.dev/api/admin-rest/2022-10/resources/order)                 |
| shopify_order_line_items | Every minute  | [Order line items](https://shopify.dev/api/storefront/2022-07/objects/orderlineitem) |
| shopify_customers        | Every minute  | [Customers](https://shopify.dev/api/storefront/2022-07/objects/Customer)             |

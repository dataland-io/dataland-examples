# Overview

Use Dataland as an admin panel into your Stripe data. See all key Stripe objects as Dataland tables, and take actions like adjusting subscription quantities, and issuing refunds + credits to customers.

### Tables

This module replicates the following objects from Stripe into Dataland as tables:

| Tables                    | Sync schedule | Stripe object                                                               |
| ------------------------- | ------------- | --------------------------------------------------------------------------- |
| stripe-customers          | Every 15 mins | [Customers](https://stripe.com/docs/api/customers/object)                   |
| stripe-subscriptions      | Every 15 mins | [Subscriptions](https://stripe.com/docs/api/subscriptions/object)           |
| stripe-subscription-items | Every 15 mins | [Subscription items](https://stripe.com/docs/api/subscription_items/object) |
| stripe-invoices           | Every 15 mins | [Invoices](https://stripe.com/docs/api/invoices/object)                     |
| stripe-payment-intents    | Every 15 mins | [Payment intents](https://stripe.com/docs/api/payment_intents/object)       |
| stripe-refunds            | Every 15 mins | [Refunds](https://stripe.com/docs/api/refunds/object)                       |

### Actions

This module contains several workers for actions:

| Purpose                       | Worker file                                      | How to trigger                                        |
| ----------------------------- | ------------------------------------------------ | ----------------------------------------------------- |
| Increment a subscription item | `postStripeSubscriptionItemQuantityIncrement.ts` | Click `Increment` button on stripe-subscription-items |
| Decrement a subscription item | `postStripeSubscriptionItemQuantityDecrement.ts` | Click `Decrement` button on stripe-subscription-items |
| Refund a payment              | `postStripePaymentIntentRefund.ts`               | Click `Refund` button on stripe-payment-intents       |

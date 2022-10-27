# Overview

Use Dataland as an admin panel into your Stripe data. See all key Stripe objects as Dataland tables, and take actions like adjusting subscription quantities, and issuing refunds + credits to customers.

### Tables

This module replicates the following objects from Stripe into Dataland as tables:

| Tables                    | Sync schedule | Stripe object                                                               |
| ------------------------- | ------------- | --------------------------------------------------------------------------- |
| stripe_customers          | Every 15 mins | [Customers](https://stripe.com/docs/api/customers/object)                   |
| stripe_subscriptions      | Every 15 mins | [Subscriptions](https://stripe.com/docs/api/subscriptions/object)           |
| stripe_subscription-items | Every 15 mins | [Subscription items](https://stripe.com/docs/api/subscription_items/object) |
| stripe_invoices           | Every 15 mins | [Invoices](https://stripe.com/docs/api/invoices/object)                     |
| stripe_payment-intents    | Every 15 mins | [Payment intents](https://stripe.com/docs/api/payment_intents/object)       |
| stripe_refunds            | Every 15 mins | [Refunds](https://stripe.com/docs/api/refunds/object)                       |

### Actions

This module contains several workers for actions:

| Purpose                       | Worker file                                      | How to trigger                                        |
| ----------------------------- | ------------------------------------------------ | ----------------------------------------------------- |
| Increment a subscription item | `postStripeSubscriptionItemQuantityIncrement.ts` | Click `Increment` button on stripe_subscription_items |
| Decrement a subscription item | `postStripeSubscriptionItemQuantityDecrement.ts` | Click `Decrement` button on stripe_subscription_items |
| Refund a payment              | `postStripePaymentIntentRefund.ts`               | Click `Refund` button on stripe_payment_intents       |

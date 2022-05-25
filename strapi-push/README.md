# Pushing content to Strapi

Dataland can be a better admin panel for a headless CMS, like Strapi (used in this example), Ghost, or Contentful. Dataland gives you an intuitive table-like UI to edit data, and smart content enrichment via Dataland workers.

This directory consists of:

- A table that maps onto a Strapi CMS collection
- A worker that upserts a row's data in Dataland into the Strapi CMS. It gets triggered when a user clicks an in-line table button.

Here's how it looks in practice:

// REPLACE

For this example:

- The Strapi CMS is hosted here: https://strapi-cms-example.herokuapp.com/
- An example store front-end that lists a Strapi item collection: https://strapi-cms-app.vercel.app/

Running through the below setup will allow your Dataland DB to post / update entries in the Strapi CMS. Periodically, the Dataland core team may clear out Strapi entries.

If you'd like to construct your own private Strapi CMS for testing, follow the quickstart on Strapi's docs: https://docs.strapi.io/developer-docs/.

## Setup instructions

To run this example, clone this repo . Then, from the `strapi-push/` folder, run:

```sh
npm install
dataland deploy
```

Then do the following:

1. Follow each of the "TODO"s in the `strapiPush.ts` file
2. Follow each of the "TODO"s in the `spec.yaml` file.
3. Authenticate to your workspace with `dataland config init` and your access key. You can get an access key by navigating to Settings > Access keys.
4. Run `dataland deploy`, then `dataland tail` to start streaming logs.
5. Go to the Dataland app, add a row to the Strapi Items table, and click the "Push to CMS" button to make an API POST request with that row's data into the Strapi CMS.
6. If you've kept the same endpoint as the example, you'll see the item appear on https://strapi-cms-app.vercel.app/.

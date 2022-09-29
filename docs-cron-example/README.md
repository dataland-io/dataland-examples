# Overview

This repo shows an example of how to use cron triggers in order to trigger worker execution on a recurring schedule.

The example worker inserts a row of random user data into a table every 15 seconds, like so:

![GIF example](cron-trigger.gif)

# Setup

To run this example, clone this repo, and from the `docs-cron-example/` folder, run:

```sh
npm install
dataland deploy
```

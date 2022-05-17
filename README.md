# Dataland Examples

This is a collection of example database applications that can be built built on top of the Dataland platform.
You can learn more about Dataland at <docs.dataland.io>.

## Getting started

Deploying the Quickstart example to your Dataland workspace is as easy as:

```sh
cd docs-quickstart
npm install
dataland deploy
```

Deploying any other example should be similarly easy, though if the example application integrates
with an external API you may be prompted to provide an API key.

## Repo structure

Generally speaking, each example lives in its own top-level directory in this repo
and should be fully self-contained within that directory.

There is also some additional tooling to ensure consistency across the examples.

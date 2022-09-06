---
to: <%= h.changeCase.param(name) %>/package.json
---

{
  "name": "@dataland-examples/<%= h.changeCase.param(name) %>",
  "version": "1.0.0",
  "private": true,
  "license": "MIT",
  "scripts": {
    "build": "webpack --config webpack.config.ts"
  },
  "dependencies": {
    "@dataland-io/dataland-sdk-worker": "0.16.0"
  },
  "devDependencies": {
    "@types/webpack": "5.28.0",
    "ts-loader": "9.2.8",
    "ts-node": "10.7.0",
    "typescript": "4.6.3",
    "webpack": "5.70.0",
    "webpack-cli": "4.9.2"
  }
}

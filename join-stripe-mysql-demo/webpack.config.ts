import * as path from "path";
import type { Configuration } from "webpack";

const config: Configuration = {
  mode: "production",
  target: "web",
  entry: {
<<<<<<<< HEAD:join-stripe-mysql-demo/webpack.config.ts
    joinStripeMySQLDemo: "./src/joinStripeMySQLDemo.ts",
    postStripeCredit: "./src/postStripeCredit.ts",
========
    mysqlSync: "./src/mysqlSync.ts",
>>>>>>>> main:mysql-sync/webpack.config.ts
  },
  module: {
    rules: [
      {
        test: /\.ts$/,
        use: "ts-loader",
      },
    ],
  },
  resolve: {
    extensions: [".ts", ".js"],
  },
  output: {
    path: path.resolve(__dirname, "dist"),
    filename: "[name].bundle.js",
    clean: true,
  },
  performance: {
    maxAssetSize: 10_000_000,
    maxEntrypointSize: 10_000_000,
  },
  optimization: {
    // for pretty stacktraces
    minimize: false,
  },
  experiments: {
    futureDefaults: true,
  },
};

export default config;

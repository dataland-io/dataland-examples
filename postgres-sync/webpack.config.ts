import * as path from "path";
import type { Configuration } from "webpack";

const config: Configuration = {
  mode: "production",
  target: "web",
  entry: {
    "postgres-sync": "./src/postgres-sync.ts",
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

// eslint-disable-next-line import/no-default-export
export default config;

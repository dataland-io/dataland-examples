import * as path from "path";
import type { Configuration } from "webpack";

const config: Configuration = {
  mode: "production",
  target: "web",
  entry: {
    insertExample: "./src/insertExample.ts",
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
};

export default config;

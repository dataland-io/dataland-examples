import * as path from "path";
import * as fs from "fs";
import type { Configuration } from "webpack";

const entrypointsDir = path.resolve(__dirname, "src", "workers");

const findEntrypoints = (): Record<string, string> => {
  const files = fs.readdirSync(entrypointsDir, {
    encoding: "utf8",
    withFileTypes: true,
  });
  const entrypoints: Record<string, string> = {};
  for (const file of files) {
    if (
      file.isFile() &&
      (file.name.endsWith(".ts") || file.name.endsWith(".js"))
    ) {
      const noExt = path.parse(file.name).name;
      entrypoints[noExt] = path.resolve(entrypointsDir, file.name);
    }
  }
  return entrypoints;
};

const entrypoints = findEntrypoints();
console.log("Found webpack entrypoints:");
console.log(JSON.stringify(entrypoints, null, 2));

const config: Configuration = {
  mode: "production",
  target: "web",
  entry: entrypoints,
  module: {
    rules: [
      {
        test: (f) => f.endsWith(".ts"),
        use: "ts-loader",
        exclude: (f) => f.includes("node_modules"),
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

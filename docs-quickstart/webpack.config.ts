import * as path from "path";
import * as webpack from "webpack";
import * as fs from "fs";

const entrypointsDir = path.resolve(__dirname, "..", "src", "workers");

const findEntrypoints = (): Record<string, string> => {
  const files = fs.readdirSync(entrypointsDir, {
    encoding: "utf8",
    withFileTypes: true,
  });
  const binaries: Record<string, string> = {};
  for (const file of files) {
    if (
      file.isFile() &&
      (file.name.endsWith(".ts") || file.name.endsWith(".js"))
    ) {
      const noExt = path.parse(file.name).name;
      binaries[noExt] = path.resolve(entrypointsDir, file.name);
    }
  }
  return binaries;
};

const entrypoints = findEntrypoints();
console.log("Found webpack entrypoints:");
console.log(JSON.stringify(entrypoints, null, 2));

const config: webpack.Configuration = {
  mode: "production",
  target: "web",
  entry: entrypoints,
  module: {
    rules: [
      {
        test: /\.tsx?$/,
        use: "ts-loader",
        exclude: /node_modules/,
      },
    ],
  },
  resolve: {
    extensions: [".ts", ".js"],
  },
  output: {
    path: path.resolve(__dirname, "..", "dist"),
    filename: "[name].bundle.js",
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

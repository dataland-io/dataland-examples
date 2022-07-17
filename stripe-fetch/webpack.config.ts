import * as path from "path";
import type { Configuration } from "webpack";

const config: Configuration = {
  mode: "production",
  target: "web",
  entry: {
    fetchStripeCustomers: "./src/fetchStripeCustomers.ts",
    fetchStripeSubscriptions: "./src/fetchStripeSubscriptions.ts",
    fetchStripeInvoices: "./src/fetchStripeInvoices.ts",
    fetchStripeRefunds: "./src/fetchStripeRefunds.ts",
    fetchStripePaymentIntents: "./src/fetchStripePaymentIntents.ts",
    fetchStripeCustomersWithSub: "./src/fetchStripeCustomersWithSub.ts",
    postStripeSubscriptionItemQuantityDecrement:
      "./src/postStripeSubscriptionItemQuantityDecrement.ts",
    postStripeSubscriptionItemQuantityIncrement:
      "./src/postStripeSubscriptionItemQuantityIncrement.ts",
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

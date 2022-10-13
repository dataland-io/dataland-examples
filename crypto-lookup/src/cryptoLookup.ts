import {
  getHistoryClient,
  getDbClient,
  MutationsBuilder,
  getEnv,
  registerTransactionHandler,
  Transaction,
  unpackRows,
} from "@dataland-io/dataland-sdk";

import { get } from "lodash-es";

const getPrices = async (symbols: string[]): Promise<Map<string, number>> => {
  if (symbols.length === 0) {
    return new Map<string, number>();
  }

  const apiKey = getEnv("COINMARKETCAP_API_KEY");
  if (apiKey == null) {
    throw new Error("Missing environment variable - COINMARKETCAP_API_KEY");
  }

  const api = new URL(
    "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest"
  );
  const params = {
    symbol: symbols.join(","),
    convert: "USD",
  };
  api.search = new URLSearchParams(params).toString();

  const response = await fetch(api.href, {
    headers: {
      "X-CMC_PRO_API_KEY": apiKey,
    },
  });
  if (!response.ok) {
    const responseText = await response.text();
    console.error(
      "coinmarketcap api call failed",
      response.status,
      responseText,
      symbols
    );
    throw new Error("Failed to fetch crypto prices");
  }

  const json = await response.json();

  console.log("coinmarketcap api call succeeded", json);

  const prices = new Map<string, number>();
  for (const symbol of symbols) {
    const upper = symbol.toUpperCase();
    const price = get(json, ["data", upper, "quote", "USD", "price"]);
    prices.set(symbol, price);
  }

  return prices;
};

const handler = async (transaction: Transaction) => {
  const db = await getDbClient();
  const history = await getHistoryClient();

  const response = await history.querySqlSnapshot({
    logicalTimestamp: transaction.logicalTimestamp,
    sqlQuery: "select _row_id, symbol from crypto",
  }).response;

  const rows = unpackRows(response);

  const symbols: Map<number, string> = new Map();
  for (const row of rows) {
    const key = Number(row["_row_id"]);
    let symbol = String(row["symbol"]);
    symbol = symbol.trim();
    if (symbol.length === 0) {
      continue;
    }
    symbols.set(key, symbol);
  }

  const priceBySymbol = await getPrices([...symbols.values()]);

  for (const [key, symbol] of symbols) {
    const price = priceBySymbol.get(symbol);
    if (price == null) {
      continue;
    }
    // mutations write
    await new MutationsBuilder()
      .updateRow("crypto", key, {
        price: price,
      })
      .run(db);
  }
};

registerTransactionHandler(handler);

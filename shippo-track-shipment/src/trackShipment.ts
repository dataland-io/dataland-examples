import {
  getCatalogSnapshot,
  getEnv,
  Mutation,
  querySqlSnapshot,
  registerTransactionHandler,
  runMutations,
  Scalar,
  Schema,
  Transaction,
  unpackRows,
} from "@dataland-io/dataland-sdk";
import * as t from "io-ts";

const TrackingStatusT = t.partial({
  eta: t.string,
  original_eta: t.string,
  tracking_status: t.partial({
    status: t.string,
    status_details: t.string,
    status_date: t.string,
  }),
});

type TrackingStatus = t.TypeOf<typeof TrackingStatusT>;

const fetchTrackingStatus = async (
  shippoToken: string,
  carrier: string,
  trackingNumber: string
): Promise<TrackingStatus | null> => {
  // https://goshippo.com/docs/reference/bash#tracks-retrieve

  const url = `https://api.goshippo.com/tracks/${carrier}/${trackingNumber}`;

  const response = await fetch(url, {
    method: "GET",
    headers: {
      Authorization: `ShippoToken ${shippoToken}`,
    },
  });
  if (!response.ok) {
    const responseText = await response.text();
    console.error(
      "shippo api call failed",
      response.status,
      responseText,
      carrier,
      trackingNumber
    );
    return null;
  }
  const json: unknown = await response.json();

  if (TrackingStatusT.is(json)) {
    return json;
  } else {
    console.error("shippo api response malformed", json);
    return null;
  }
};

const handler = async (transaction: Transaction) => {
  const shippoToken = getEnv("SHIPPO_API_TOKEN");

  const { tableDescriptors } = await getCatalogSnapshot({
    logicalTimestamp: transaction.logicalTimestamp,
  });
  const schema = new Schema(tableDescriptors);

  const affectedRows = schema.getAffectedRows(
    "shipments",
    "Lookup Tracking Number",
    transaction
  );

  const lookupKeys: number[] = [];
  for (const [key, value] of affectedRows) {
    if (typeof value === "string" && value.trim().length > 0) {
      lookupKeys.push(key);
    }
  }

  if (lookupKeys.length === 0) {
    return;
  }

  interface OutputColumn {
    name: string;
    getValue: (trackingStatus: TrackingStatus) => Scalar;
  }
  const outputColumns: OutputColumn[] = [
    {
      name: "Status",
      getValue: (trackingStatus) =>
        trackingStatus.tracking_status?.status ?? null,
    },
    {
      name: "Status Details",
      getValue: (trackingStatus) =>
        trackingStatus.tracking_status?.status_details ?? null,
    },
    {
      name: "Current ETA",
      getValue: (trackingStatus) => trackingStatus.eta ?? null,
    },
    {
      name: "Original ETA",
      getValue: (trackingStatus) => trackingStatus.original_eta ?? null,
    },
    {
      name: "ETA Difference Hours",
      getValue: (trackingStatus) => {
        const currentEta = trackingStatus.eta;
        const originalEta = trackingStatus.original_eta;
        if (currentEta == null || originalEta == null) {
          return null;
        }
        const currentEtaMillis = Date.parse(currentEta);
        const originalEtaMillis = Date.parse(originalEta);
        if (Number.isNaN(currentEtaMillis) || Number.isNaN(originalEtaMillis)) {
          console.error("Failed to parse dates", currentEta, originalEta);
          return null;
        }
        const etaDiffMillis = currentEtaMillis - originalEtaMillis;
        const etaDiffHours = Math.floor(etaDiffMillis / (60 * 60 * 1000));
        return etaDiffHours;
      },
    },
  ];

  const outputColumnExists: Record<string, boolean> = {};
  let someOutputColumnExists = false;
  for (const { name } of outputColumns) {
    try {
      schema.getColumnDescriptor("shipments", name);
      outputColumnExists[name] = true;
      someOutputColumnExists = true;
    } catch (e) {
      outputColumnExists[name] = false;
    }
  }

  if (!someOutputColumnExists) {
    return;
  }

  const keyList = `(${lookupKeys.join(",")})`;
  const response = await querySqlSnapshot({
    logicalTimestamp: transaction.logicalTimestamp,
    sqlQuery: `
      select
        _dataland_key,
        "Lookup Tracking Number"
      from shipments
      where _dataland_key in ${keyList}
    `,
  });

  const rows = unpackRows(response);

  const getOutputValues = (
    trackingStatus: TrackingStatus
  ): Record<string, Scalar> => {
    const outputValues: Record<string, Scalar> = {};
    for (const { name, getValue } of outputColumns) {
      if (outputColumnExists[name] === true) {
        outputValues[name] = getValue(trackingStatus);
      }
    }
    return outputValues;
  };

  const mutations: Mutation[] = [];
  for (const row of rows) {
    const key = Number(row["_dataland_key"]);
    const trackingNumber = String(row["Lookup Tracking Number"]).trim();
    if (trackingNumber.length === 0) {
      continue;
    }
    const trackingStatus = await fetchTrackingStatus(
      shippoToken,
      "usps",
      trackingNumber
    );
    if (trackingStatus == null) {
      continue;
    }
    const outputValues = getOutputValues(trackingStatus);
    const update = schema.makeUpdateRows("shipments", key, outputValues);
    mutations.push(update);
  }

  if (mutations.length === 0) {
    return;
  }

  await runMutations({ mutations });
};

registerTransactionHandler(handler);

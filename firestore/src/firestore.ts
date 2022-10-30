import { z } from "zod";

// Firestore REST APIs rely on gRPC/HTTP transcoding.
//
// For example for ListDocuments see:
// https://firebase.google.com/docs/firestore/reference/rest/v1beta1/projects.databases.documents/list
//
// The gRPC/HTTP transcoding is defined here:
// https://google.aip.dev/127
// https://cloud.google.com/endpoints/docs/grpc/transcoding
//
// The transcoding process relies on a Protobuf-to-JSON mapping which is defined here:
// https://developers.google.com/protocol-buffers/docs/proto3#json
//
// The below is just the general transcoding process applied to the Firestore gRPC API defined here:
// https://github.com/googleapis/googleapis/blob/master/google/firestore/v1
//
// This can also been seen in the firebase-js-sdk:
// https://github.com/firebase/firebase-js-sdk/blob/master/packages/firestore/src/protos/firestore_proto_api.ts

export type NullValue = {
  nullValue: null;
};
export type BooleanValue = {
  booleanValue: boolean;
};
export type IntegerValue = {
  // integerValue is `int64` in protobuf which is mapped to `string` in JSON
  integerValue: string;
};
export type DoubleValue = {
  doubleValue: number;
};
export type TimestampValue = {
  timestampValue: string;
};
export type StringValue = {
  stringValue: string;
};
export type BytesValue = {
  bytesValue: string;
};
export type ReferenceValue = {
  referenceValue: string;
};
export type GeoPointValue = {
  geoPointValue: {
    latitude: number;
    longitude: number;
  };
};

export type ArrayValue = { arrayValue: { values: Value[] } };
export type MapValue = { mapValue: { fields: Record<string, Value> } };

export type Value =
  | NullValue
  | BooleanValue
  | IntegerValue
  | DoubleValue
  | TimestampValue
  | StringValue
  | BytesValue
  | ReferenceValue
  | GeoPointValue
  | ArrayValue
  | MapValue;

export const NullValue: z.ZodType<NullValue> = z.object({
  nullValue: z.null(),
});
export const BooleanValue: z.ZodType<BooleanValue> = z.object({
  booleanValue: z.boolean(),
});
export const IntegerValue: z.ZodType<IntegerValue> = z.object({
  integerValue: z.string(),
});
export const DoubleValue: z.ZodType<DoubleValue> = z.object({
  doubleValue: z.number(),
});
export const TimestampValue: z.ZodType<TimestampValue> = z.object({
  timestampValue: z.string(),
});
export const StringValue: z.ZodType<StringValue> = z.object({
  stringValue: z.string(),
});
export const BytesValue: z.ZodType<BytesValue> = z.object({
  bytesValue: z.string(),
});
export const ReferenceValue: z.ZodType<ReferenceValue> = z.object({
  referenceValue: z.string(),
});
export const GeoPointValue: z.ZodType<GeoPointValue> = z.object({
  geoPointValue: z.object({ latitude: z.number(), longitude: z.number() }),
});

export const ArrayValue: z.ZodType<ArrayValue> = z.lazy(() =>
  z.object({ arrayValue: z.object({ values: z.array(Value) }) })
);
export const MapValue: z.ZodType<MapValue> = z.lazy(() =>
  z.object({ mapValue: z.object({ fields: z.record(Value) }) })
);

export const Value: z.ZodType<Value> = z.lazy(() =>
  z.union([
    NullValue,
    BooleanValue,
    IntegerValue,
    DoubleValue,
    TimestampValue,
    StringValue,
    BytesValue,
    ReferenceValue,
    GeoPointValue,
    ArrayValue,
    MapValue,
  ])
);

export const Context = z.object({
  projectId: z.string(),
  authHeader: z.string(),
});

export type Context = z.infer<typeof Context>;

export const Document = z.object({
  name: z.string(),
  fields: z.record(Value).optional(),
  createTime: z.string().optional(),
  updateTime: z.string().optional(),
});

export type Document = z.infer<typeof Document>;

export const DocumentMask = z.object({
  fieldPaths: z.array(z.string()),
});

export type DocumentMask = z.infer<typeof DocumentMask>;

export const Write = z.union([
  z.object({
    update: Document,
    updateMask: DocumentMask.optional(),
  }),
  z.object({
    delete: z.string(),
  }),
]);

export type Write = z.infer<typeof Write>;

export const TransactionOptions = z.union([
  z.object({
    readOnly: z.object({
      readTime: z.string().optional(),
    }),
  }),
  z.object({
    readWrite: z.object({
      retryTransaction: z.string().optional(),
    }),
  }),
]);

export type TransactionOptions = z.infer<typeof TransactionOptions>;

export const BatchGetFound = z.object({
  found: Document,
});

export type BatchGetFound = z.infer<typeof BatchGetFound>;

export const BatchGetMissing = z.object({
  missing: z.string(),
});

export type BatchGetMissing = z.infer<typeof BatchGetMissing>;

export const BatchGetResult = z.union([BatchGetFound, BatchGetMissing]);

export type BatchGetResult = z.infer<typeof BatchGetResult>;

const ListDocumentsResponse = z.object({
  documents: z.array(Document),
  nextPageToken: z.string().optional(),
});

type ListDocumentsResponse = z.infer<typeof ListDocumentsResponse>;

const BeginTransactionResponse = z.object({
  transaction: z.string(),
});

type BeginTransactionResponse = z.infer<typeof BeginTransactionResponse>;

const CommitResponse = z.object({
  commitTime: z.string(),
});

type CommitResponse = z.infer<typeof CommitResponse>;

// https://firebase.google.com/docs/firestore/reference/rest/v1/projects.databases.documents/beginTransaction
export async function beginTransaction(
  options: TransactionOptions,
  context: Context
): Promise<string> {
  const url = new URL(
    `https://firestore.googleapis.com/v1/projects/${context.projectId}/databases/(default)/documents:beginTransaction`
  );
  const body = {
    options,
  };
  const response = await fetch(url, {
    headers: {
      authorization: context.authHeader,
    },
    method: "POST",
    body: JSON.stringify(body),
  });
  const responseJson = await response.json();
  const parsedResponse = BeginTransactionResponse.parse(responseJson);
  return parsedResponse.transaction;
}

// https://firebase.google.com/docs/firestore/reference/rest/v1/projects.databases.documents/commit
export async function commit(
  transaction: string,
  writes: Write[],
  context: Context
): Promise<string> {
  const url = new URL(
    `https://firestore.googleapis.com/v1/projects/${context.projectId}/databases/(default)/documents:commit`
  );
  const body = {
    transaction,
    writes,
  };
  const response = await fetch(url, {
    headers: {
      authorization: context.authHeader,
    },
    method: "POST",
    body: JSON.stringify(body),
  });
  const responseJson = await response.json();
  const parsedResponse = CommitResponse.parse(responseJson);
  return parsedResponse.commitTime;
}

// https://firebase.google.com/docs/firestore/reference/rest/v1/projects.databases.documents/list
export async function listDocuments(
  collectionId: string,
  context: Context
): Promise<Document[]> {
  const transaction = await beginTransaction({ readOnly: {} }, context);

  const documents: Document[] = [];
  let nextPageToken: string | null = null;
  // eslint-disable-next-line no-constant-condition
  while (true) {
    const url = new URL(
      `https://firestore.googleapis.com/v1/projects/${context.projectId}/databases/(default)/documents/${collectionId}`
    );
    url.searchParams.set("transaction", transaction);
    if (nextPageToken != null) {
      url.searchParams.set("pageToken", nextPageToken);
    }
    const response = await fetch(url, {
      headers: {
        authorization: context.authHeader,
      },
    });
    const responseJson = await response.json();
    const parsedResponse = ListDocumentsResponse.parse(responseJson);
    for (const document of parsedResponse.documents) {
      documents.push(document);
    }
    if (
      parsedResponse.nextPageToken == null ||
      parsedResponse.nextPageToken === ""
    ) {
      break;
    } else {
      nextPageToken = parsedResponse.nextPageToken;
    }
  }

  await commit(transaction, [], context);

  return documents;
}

const UnknownRecordArray = z.array(z.record(z.unknown()));

// https://firebase.google.com/docs/firestore/reference/rest/v1/projects.databases.documents/batchGet
export async function batchGetDocuments(
  documentNames: string[],
  context: Context
): Promise<BatchGetResult[]> {
  const url = new URL(
    `https://firestore.googleapis.com/v1/projects/${context.projectId}/databases/(default)/documents:batchGet`
  );
  const transactionOptions: TransactionOptions = { readOnly: {} };
  const body = {
    documents: documentNames,
    newTransaction: transactionOptions,
  };
  const response = await fetch(url, {
    headers: {
      authorization: context.authHeader,
    },
    method: "POST",
    body: JSON.stringify(body),
  });
  const responseJson = await response.json();
  const parsedResponse = UnknownRecordArray.parse(responseJson);
  const batchGetResults: BatchGetResult[] = [];
  for (const item of parsedResponse) {
    if ("transaction" in item) {
      continue;
    }
    const batchGetResult: BatchGetResult = BatchGetResult.parse(item);
    batchGetResults.push(batchGetResult);
  }
  return batchGetResults;
}

export const isNullValue = (value: Value): value is NullValue => {
  return "nullValue" in value;
};

export const isBooleanValue = (value: Value): value is BooleanValue => {
  return "booleanValue" in value;
};

export const isIntegerValue = (value: Value): value is IntegerValue => {
  return "integerValue" in value;
};

export const isDoubleValue = (value: Value): value is DoubleValue => {
  return "doubleValue" in value;
};

export const isTimestampValue = (value: Value): value is TimestampValue => {
  return "timestampValue" in value;
};

export const isStringValue = (value: Value): value is StringValue => {
  return "stringValue" in value;
};

export const isBytesValue = (value: Value): value is BytesValue => {
  return "bytesValue" in value;
};

export const isReferenceValue = (value: Value): value is ReferenceValue => {
  return "referenceValue" in value;
};

export const isGeoPointValue = (value: Value): value is GeoPointValue => {
  return "geoPointValue" in value;
};

export const isArrayValue = (value: Value): value is ArrayValue => {
  return "arrayValue" in value;
};

export const isMapValue = (value: Value): value is MapValue => {
  return "mapValue" in value;
};

export const isBatchGetFound = (
  result: BatchGetResult
): result is BatchGetFound => {
  return "found" in result;
};

export const isBatchGetMissing = (
  result: BatchGetResult
): result is BatchGetMissing => {
  return "missing" in result;
};

export const getDocumentId = (documentName: string): string => {
  const parts = documentName.split("/");
  if (parts.length < 1) {
    throw new Error();
  }
  return parts[parts.length - 1];
};

export const getCollectionName = (
  collectionId: string,
  projectId: string
): string | null => {
  return `projects/${projectId}/databases/(default)/documents/${collectionId}`;
};

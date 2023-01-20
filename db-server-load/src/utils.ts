import {
  unpackRows,
  ResultRow,
  IDbServiceClient,
  assertNever,
} from "@dataland-io/dataland-sdk";

// Load's a table's data from db-server.
export const loadTable = async (
  db: IDbServiceClient,
  tableName: string
): Promise<ResultRow[]> => {
  const arrowRecordBatches: Uint8Array[] = [];
  const arrowLoadTableResponse = db.arrowLoadTable({ tableName });
  for await (const response0 of arrowLoadTableResponse.responses) {
    const response = response0.kind;
    if (response.oneofKind == null) {
      continue;
    }
    if (response.oneofKind === "start") {
      // handle start
    } else if (response.oneofKind === "data") {
      // handle data
      arrowRecordBatches.push(...response.data.arrowRecordBatches);
    } else if (response.oneofKind === "finish") {
      // handle finish
    } else {
      assertNever(response);
    }
  }
  return unpackRows({ arrowRecordBatches });
};

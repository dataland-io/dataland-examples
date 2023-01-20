import {
  getDbClient,
  unpackRows,
  registerCronHandler,
  ResultRow,
} from "@dataland-io/dataland-sdk";

const handler = async () => {
  const db = await getDbClient();
  const table = await db.arrowLoadTable({ tableName: "fruits" }).responses;

  // message: { kind: { oneofKind: "data", data: { arrowRecordBatches: [ [Uint8Array] ] } } }
  // table.onMessage((message) => console.log(message));

  // if table.onMessage's one of kind is data, store in array
  const arrowRecordBatches: Uint8Array[] = [];
  table.onMessage((message) => {
    if (message.kind.oneofKind === "data") {
      for (const batch of message.kind.data.arrowRecordBatches) {
        console.log("batch:", batch);
        arrowRecordBatches.push(batch);
      }
    }
  });

  let rows: ResultRow[] = [];
  // Todo: Need to be able to use the rows object outside of the onComplete callback
  table.onComplete(() => {
    console.log("arrowRecordBatches:", arrowRecordBatches);
    rows = unpackRows({ arrowRecordBatches: arrowRecordBatches });
    console.log("rows inner:", rows);
  });

  console.log("rows outer:", rows);
};

registerCronHandler(handler);

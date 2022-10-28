import {
  ListValue,
  Mutation,
  MutationsBuilder,
  TableSyncRequest,
  Transaction,
  Value,
  getDbClient,
  getEnv,
  isObjectEmpty,
  registerCronHandler,
  registerTransactionHandler,
} from "@dataland-io/dataland-sdk";
import { tableFromJSON, tableToIPC } from "apache-arrow";
import {
  Context,
  Document,
  Write,
  batchGetDocuments,
  beginTransaction,
  commit,
  getCollectionName,
  isBatchGetFound,
  isBatchGetMissing,
  listDocuments,
} from "./firestore";
import { createOAuth2Token } from "./gcpAuth";
import { DOCUMENT_ID_COLUMN, TYPE_DEFINITIONS } from "./schema";
import {
  GenericDocType,
  GenericRowType,
  TypeDefinition,
} from "./schema/typeDefinition";

// can also use getEnv to make this pluggable

const SERVICE_ACCOUNT_KEYS_JSON = getEnv("SERVICE_ACCOUNT_KEYS_JSON");
const SERVICE_ACCOUNT_KEYS_JSON_OBJ = JSON.parse(SERVICE_ACCOUNT_KEYS_JSON);
export const PROJECT_ID = SERVICE_ACCOUNT_KEYS_JSON_OBJ.project_id;

const db = getDbClient();

const cronHandler = async () => {
  const context = await getContext();

  for (const ingestType of Object.values(TYPE_DEFINITIONS)) {
    await ingestTable(ingestType, context);
  }
};

registerCronHandler(cronHandler);

const getContext = async () => {
  const serviceAccountKeysJson = getEnv("SERVICE_ACCOUNT_KEYS_JSON");
  const serviceAccountKeys = JSON.parse(serviceAccountKeysJson);
  const oauth2Token = await createOAuth2Token(
    serviceAccountKeys,
    "https://www.googleapis.com/auth/datastore"
  );
  const context: Context = {
    projectId: PROJECT_ID,
    authHeader: oauth2Token.toString(),
  };
  return context;
};

type Backlog = {
  updated: Record<string, true>;
  deleted: Record<string, true>;
};
type Backlogs = Record<string, Backlog>;
const Backlogs: Backlogs = {};

async function ingestTable<
  DocType extends GenericDocType,
  RowType extends GenericRowType
>(typeDef: TypeDefinition<DocType, RowType>, context: Context): Promise<void> {
  const backlog: Backlog = { updated: {}, deleted: {} };
  Backlogs[typeDef.collectionId] = backlog;

  try {
    console.log("starting to ingest table", {
      collectionId: typeDef.collectionId,
      tableName: typeDef.tableName,
    });
    const startTime = Date.now();

    const documents = await listDocuments(typeDef.collectionId, context);

    console.log("fetched documents", documents);

    let numFailed = 0;
    const rows: Map<string, RowType> = new Map();

    const processDocument = (document: Document) => {
      try {
        const doc = typeDef.docType.parse(document.fields ?? {});
        const row = typeDef.docToRow(doc, document);
        rows.set(document.name, row);
      } catch (e) {
        // TODO(hzuo): Insert into quarantine table
        console.warn("ignoring malformed document", document, e);
        numFailed++;
      }
    };

    for (const document of documents) {
      processDocument(document);
    }

    // eslint-disable-next-line no-constant-condition
    for (let backlogIteration = 1; ; backlogIteration++) {
      if (isObjectEmpty(backlog.updated) && isObjectEmpty(backlog.deleted)) {
        break;
      }
      console.log("found additional items in backlog", {
        backlog,
        backlogIteration,
      });
      for (const documentName in backlog.deleted) {
        rows.delete(documentName);
      }
      backlog.deleted = {};
      if (isObjectEmpty(backlog.updated)) {
        break;
      }
      const updated = Object.keys(backlog.updated);
      backlog.updated = {};
      const batchGetResults = await batchGetDocuments(updated, context);
      for (const batchGetResult of batchGetResults) {
        if (isBatchGetFound(batchGetResult)) {
          processDocument(batchGetResult.found);
        } else if (isBatchGetMissing(batchGetResult)) {
          rows.delete(batchGetResult.missing);
        } else {
          throw new Error();
        }
      }
    }

    const fetchEndTime = Date.now();

    const rowArray = Array.from(rows.values());
    const arrowTable = tableFromJSON(rowArray);
    const arrowRecordBatch = tableToIPC(arrowTable);
    const tableSyncRequest: TableSyncRequest = {
      tableName: typeDef.tableName,
      arrowRecordBatches: [arrowRecordBatch],
      primaryKeyColumnNames: [DOCUMENT_ID_COLUMN],
      deleteExtraRows: true,
      dropExtraColumns: false,
      tableAnnotations: {},
      columnAnnotations: {},
      transactionAnnotations: {},
    };
    await db.tableSync(tableSyncRequest);

    const writeEndTime = Date.now();

    console.log("successfully ingested table", {
      collectionId: typeDef.collectionId,
      tableName: typeDef.tableName,
      numSucceeded: rowArray.length,
      numFailed,
      totalMs: writeEndTime - startTime,
      fetchMs: fetchEndTime - startTime,
      writeMs: writeEndTime - fetchEndTime,
    });
  } finally {
    delete Backlogs[typeDef.collectionId];
  }
}

const transactionHandler = async (transaction: Transaction) => {
  const [mutations, writes] = transactionToWrites(transaction);

  if (mutations.length > 0) {
    await db.runMutations({ mutations, transactionAnnotations: {} }).response;
  }
  if (writes.length > 0) {
    console.log("propagating writes to firestore", writes.length);

    const context = await getContext();
    const firestoreTx = await beginTransaction({ readWrite: {} }, context);
    await commit(firestoreTx, writes, context);

    console.log("successfully committed writes to firestore", writes.length);
  }
};

registerTransactionHandler(transactionHandler);

const transactionToWrites = (
  transaction: Transaction
): [Mutation[], Write[]] => {
  const mutationsBuilder: MutationsBuilder = new MutationsBuilder();
  const writes: Write[] = [];

  for (const tableName in TYPE_DEFINITIONS) {
    const dataChangeRecord = transaction.dataChangeRecords[tableName];
    if (dataChangeRecord == null) {
      // This table/collection was not unaffected by this transaction.
      continue;
    }

    const typeDefinition = TYPE_DEFINITIONS[tableName]!;
    const collectionId = typeDefinition.collectionId;
    const collectionName = getCollectionName(collectionId, PROJECT_ID);

    // Handle inserts
    for (const row of dataChangeRecord.insertedRows) {
      if (row.values == null) {
        continue;
      }

      const rowObject = zipRowObject(dataChangeRecord.columnNames, row.values);

      const documentName = `${collectionName}/${row.rowId}`;

      mutationsBuilder.updateRow(tableName, row.rowId, {
        [DOCUMENT_ID_COLUMN]: String(row.rowId),
      });

      const doc = typeDefinition.rowToDoc(rowObject);

      const document: Document = {
        name: documentName,
        fields: doc,
      };
      const write: Write = {
        update: document,
        updateMask: { fieldPaths: Object.keys(doc) },
      };
      writes.push(write);

      const backlog = Backlogs[collectionId];
      if (backlog != null) {
        backlog.updated[documentName] = true;
      }
    }

    // Handle updates
    for (const row of dataChangeRecord.updatedRows) {
      if (row.values == null) {
        continue;
      }

      const rowObject = zipRowObject(dataChangeRecord.columnNames, row.values);

      const documentId = rowObject[DOCUMENT_ID_COLUMN];
      if (documentId == null) {
        console.warn(
          `Updated row is missing ${DOCUMENT_ID_COLUMN} - skipping write to Firestore`,
          rowObject
        );
        continue;
      }

      const documentName = `${collectionName}/${documentId}`;

      const doc = typeDefinition.rowToDoc(rowObject);
      if (isObjectEmpty(doc)) {
        // This update would be a no-op
        continue;
      }

      const document: Document = {
        name: documentName,
        fields: doc,
      };
      const write: Write = {
        update: document,
        updateMask: { fieldPaths: Object.keys(doc) },
      };
      writes.push(write);

      const backlog = Backlogs[collectionId];
      if (backlog != null) {
        backlog.updated[documentName] = true;
      }
    }

    // Handle deletes
    for (const row of dataChangeRecord.deletedRows) {
      if (row.values == null) {
        continue;
      }

      const rowObject = zipRowObject(dataChangeRecord.columnNames, row.values);

      const documentId = rowObject[DOCUMENT_ID_COLUMN];
      if (documentId == null) {
        console.warn(
          `Deleted row is missing ${DOCUMENT_ID_COLUMN} - skipping write to Firestore`,
          rowObject
        );
        continue;
      }

      const documentName = `${collectionName}/${documentId}`;
      const write: Write = { delete: documentName };
      writes.push(write);

      const backlog = Backlogs[collectionId];
      if (backlog != null) {
        backlog.deleted[documentName] = true;
      }
    }
  }

  const mutations = mutationsBuilder.build();
  return [mutations, writes];
};

const zipRowObject = (
  columnNames: string[],
  row: ListValue
): Record<string, unknown> => {
  const rowObject: Record<string, unknown> = {};
  for (let i = 0; i < columnNames.length; i++) {
    rowObject[columnNames[i]] = Value.toJson(row.values[i]);
  }
  return rowObject;
};

import {
  Transaction,
  getCatalogSnapshot,
  querySqlSnapshot,
  unpackRows,
  Schema,
  Mutation,
  runMutations,
  registerTransactionHandler,
} from "@dataland-io/dataland-sdk-worker";
import Airtable, {
  FieldSet as AirtableFieldSet,
  RecordData as AirtableRecordData,
  Table as AirtableTable,
} from "airtable";
import {
  AIRTABLE_API_KEY,
  AIRTABLE_BASE_ID,
  AIRTABLE_TABLE_NAME,
  DATALAND_TABLE_NAME,
  RECORD_ID,
} from "./constants";

const airtableBase = new Airtable({
  apiKey: AIRTABLE_API_KEY,
}).base(AIRTABLE_BASE_ID);
const airtableTable = airtableBase(AIRTABLE_TABLE_NAME);

const airtableUpdateRows = async (
  table: AirtableTable<AirtableFieldSet>,
  updateRows: AirtableRecordData<Partial<AirtableFieldSet>>[]
) => {
  return await new Promise((resolve, error) => {
    table.update(updateRows, { typecast: true }, (err) => {
      if (err) {
        console.error("Airtable Update Rows - Failed to update rows", {
          error: err,
        });
        error(err);
        return;
      }
      resolve(true);
    });
  });
};

const airtableCreateRows = async (
  table: AirtableTable<AirtableFieldSet>,
  createRows: { fields: AirtableFieldSet }[]
): Promise<string[]> => {
  return await new Promise((resolve, error) => {
    table.create(createRows, { typecast: true }, (err, records) => {
      if (err || records == null) {
        console.error("Airtable Create Rows - Failed to update rows", {
          error: err,
        });
        error(err);
        return;
      }

      const recordIds = records.map((record) => record.getId());
      resolve(recordIds);
    });
  });
};

const airtableDestoryRows = async (
  table: AirtableTable<AirtableFieldSet>,
  recordIds: string[]
) => {
  return await new Promise((resolve, error) => {
    table.destroy(recordIds, (err) => {
      if (err) {
        console.error("Airtable Create Rows - Failed to update rows", {
          error: err,
        });
        error(err);
        return;
      }
      resolve(true);
    });
  });
};

const handler2 = async (transaction: Transaction) => {
  const { tableDescriptors } = await getCatalogSnapshot({
    logicalTimestamp: transaction.logicalTimestamp,
  });

  const response = await querySqlSnapshot({
    logicalTimestamp: transaction.logicalTimestamp - 1,
    sqlQuery: `select "_dataland_key", "${RECORD_ID}" from "${DATALAND_TABLE_NAME}"`,
  });

  const rows = unpackRows(response);

  const recordIdMap: Record<number, string> = {};
  for (const row of rows) {
    const key = row["_dataland_key"] as number;
    const recordId = row[RECORD_ID] as string;
    recordIdMap[key] = recordId;
  }

  const tableDescriptor = tableDescriptors.find(
    (descriptor) => descriptor.tableName === DATALAND_TABLE_NAME
  );
  if (tableDescriptor == null) {
    console.error(
      "Airtable Sync - Could not find table descriptor by table name",
      { tableName: DATALAND_TABLE_NAME }
    );
    return;
  }

  const schema = new Schema(tableDescriptors);

  const getColumnName = (columnUuid: string): string | null => {
    const column = tableDescriptor.columnDescriptors.find(
      (c) => c.columnUuid === columnUuid
    );
    if (column == null) {
      return null;
    }
    return column.columnName;
  };

  for (const mutation of transaction.mutations) {
    if (
      mutation.kind !== "insert_rows" &&
      mutation.kind !== "update_rows" &&
      mutation.kind !== "delete_rows"
    ) {
      return;
    }

    if (tableDescriptor.tableUuid !== mutation.value.tableUuid) {
      continue;
    }

    switch (mutation.kind) {
      case "insert_rows": {
        const createRows: { fields: AirtableFieldSet }[] = [];
        for (let i = 0; i < mutation.value.rows.length; i++) {
          const createRow: AirtableFieldSet = {};
          const { values } = mutation.value.rows[i]!;

          for (let j = 0; j < mutation.value.rows.length; j++) {
            const rowValue = values[j];
            const columnUuid = mutation.value.columnMapping[i];
            const columnName = getColumnName(columnUuid);
            if (columnName == null) {
              console.error(
                "Airtable Sync - Could not find column by column uuid",
                { columnUuid }
              );
              return;
            }
            if (columnName === "_dataland_ordinal") {
              continue;
            }
            createRow[columnName] = rowValue?.value;
          }

          createRows.push({ fields: createRow });
        }

        const recordIds = await airtableCreateRows(airtableTable, createRows);
        const mutations: Mutation[] = [];
        for (let i = 0; i < recordIds.length; i++) {
          const recordId = recordIds[i];
          const datalandKey = mutation.value.rows[i]?.key;

          if (datalandKey == null) {
            console.error(
              "Airtable Sync - Could not find dataland key for record id",
              { recordId }
            );
            return;
          }

          const update = schema.makeUpdateRows(
            DATALAND_TABLE_NAME,
            datalandKey,
            {
              [RECORD_ID]: recordId,
            }
          );
          mutations.push(update);
        }
        await runMutations({ mutations });

        break;
      }
      case "update_rows": {
        const updateRows: AirtableRecordData<Partial<AirtableFieldSet>>[] = [];
        for (let i = 0; i < mutation.value.rows.length; i++) {
          const updateRow: Partial<AirtableFieldSet> = {};
          const { key, values } = mutation.value.rows[i]!;

          for (let j = 0; j < mutation.value.rows.length; j++) {
            const rowValue = values[j];
            const columnUuid = mutation.value.columnMapping[i];
            const columnName = getColumnName(columnUuid);
            if (columnName == null) {
              console.error(
                "Airtable Sync - Could not find column by column uuid",
                { columnUuid }
              );
              return;
            }
            if (columnName === "_dataland_ordinal") {
              continue;
            }
            updateRow[columnName] = rowValue?.value;
          }

          const recordId = recordIdMap[key];
          if (recordId == null) {
            console.error("Airtable Write Back - Could not find record id", {
              key,
            });
            return;
          }
          updateRows.push({ id: recordId, fields: updateRow });
        }
        await airtableUpdateRows(airtableTable, updateRows);
        break;
      }
      case "delete_rows": {
        const deleteRows: string[] = [];
        for (let i = 0; i < mutation.value.keys.length; i++) {
          const key = mutation.value.keys[i];
          const recordId = recordIdMap[key];

          if (recordId == null) {
            console.error("Airtable Sync - Could not find Airtable record id", {
              datalandKey: key,
            });
            return;
          }

          deleteRows.push(recordId);
        }

        if (deleteRows.length === 0) {
          continue;
        }
        await airtableDestoryRows(airtableTable, deleteRows);
        break;
      }
    }
  }
};

registerTransactionHandler(handler2);

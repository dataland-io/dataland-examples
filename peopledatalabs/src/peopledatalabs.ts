import {
  registerTransactionHandler,
  Transaction,
  getHistoryClient,
  getDbClient,
  unpackRows,
  getEnv,
  MutationsBuilder,
  isNumber,
  isString,
} from "@dataland-io/dataland-sdk";

const getPeopleEnrichment = async (linkedin_url: string) => {
  const PEOPLEDATALABS_API_KEY = getEnv("PEOPLEDATALABS_API_KEY");

  const options = {
    method: "GET",
    headers: {
      "Content-Type": "application/json",
      "x-api-key": PEOPLEDATALABS_API_KEY,
    },
  };

  const response = await fetch(
    `https://api.peopledatalabs.com/v5/person/enrich?pretty=True&profile=${linkedin_url}&titlecase=true`,
    options
  );
  const data = await response.json();
  console.log("data", data);
  return data;
};

const handler = async (transaction: Transaction) => {
  const affected_row_ids = [];

  for (const mutation of transaction.mutations) {
    if (mutation.kind.oneofKind == "updateRows") {
      if (
        mutation.kind.updateRows.columnNames.includes("linkedin_url") &&
        mutation.kind.updateRows.tableName === "contacts"
      ) {
        for (const row of mutation.kind.updateRows.rows) {
          affected_row_ids.push(row.rowId);
        }
      } else {
        return;
      }
    } else {
      return;
    }
  }

  const affected_row_ids_key_list = affected_row_ids.join(",");

  // get all rows where issue_refund was incremented
  const history = await getHistoryClient();
  const db = await getDbClient();
  const response = await history.querySqlMirror({
    sqlQuery: `select _row_id, linkedin_url from "contacts" where _row_id in (${affected_row_ids_key_list})`,
  }).response;

  const rows = unpackRows(response);

  if (rows == null) {
    return;
  }

  // for each row, run the logic
  for (const row of rows) {
    if (!isNumber(row._row_id) || !isString(row.linkedin_url)) {
      continue;
    }

    const linkedin_url = row.linkedin_url;
    const enrichment = await getPeopleEnrichment(linkedin_url);
    console.log("enrichment", enrichment);

    if (enrichment.status !== 200) {
      await new MutationsBuilder()
        .updateRow("contacts", row._row_id, {
          processed_at: new Date().toISOString(),
          enrichment_json: JSON.stringify(enrichment),
          status: "API Error",
        })
        .run(db);
      continue;
    }

    const enrichment_match = enrichment.data;

    const full_name = enrichment_match.first_name;
    const likelihood = String(enrichment.likelihood);
    const work_email = enrichment_match.work_email;
    const personal_emails = JSON.stringify(enrichment_match.personal_emails);
    const job_title = enrichment_match.job_title;
    const job_company_name = enrichment_match.job_company_name;

    await new MutationsBuilder()
      .updateRow("contacts", row._row_id, {
        processed_at: new Date().toISOString(),
        enrichment_json: JSON.stringify(enrichment),
        status: "API Success",
        likelihood: likelihood,
        full_name: full_name,
        work_email: work_email,
        personal_emails: personal_emails,
        job_title: job_title,
        job_company_name: job_company_name,
      })
      .run(db);
  }
};

registerTransactionHandler(handler);

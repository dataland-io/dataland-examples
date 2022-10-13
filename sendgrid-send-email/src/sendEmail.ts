import {
  getEnv,
  getDbClient,
  getHistoryClient,
  MutationsBuilder,
  registerTransactionHandler,
  Transaction,
  unpackRows,
} from "@dataland-io/dataland-sdk";
import * as t from "io-ts";

interface Mailbox {
  email: string;
  name?: string;
}

const sendEmail = async (
  sendgridApiKey: string,
  from: Mailbox,
  to: Mailbox,
  subject: string,
  body: string
): Promise<void> => {
  const requestBody = {
    personalizations: [
      {
        to: [to],
      },
    ],
    from,
    subject,
    content: [
      {
        type: "text/plain",
        value: body,
      },
    ],
  };

  const response = await fetch("https://api.sendgrid.com/v3/mail/send", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${sendgridApiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(requestBody),
  });
  if (!response.ok) {
    const responseText = await response.text();
    console.error(
      "Failed to send email via SendGrid API",
      response.status,
      responseText,
      JSON.stringify(requestBody)
    );
    throw new Error();
  }

  console.log(
    "Successfully sent email via SendGrid API",
    JSON.stringify(requestBody)
  );
};

const handler = async (transaction: Transaction) => {
  const sendgridApiKey = getEnv("SENDGRID_API_KEY");
  const fromEmail = getEnv("SENDGRID_FROM_EMAIL");
  const fromName = getEnv("SENDGRID_FROM_NAME");

  const affected_row_ids = [];

  for (const mutation of transaction.mutations) {
    if (mutation.kind.oneofKind == "updateRows") {
      if (
        mutation.kind.updateRows.columnNames.includes("send_email") &&
        mutation.kind.updateRows.tableName === "emails"
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
  console.log("xx1", affected_row_ids_key_list);

  // get all rows where issue_refund was incremented
  const history = await getHistoryClient();
  const response = await history.querySqlMirror({
    sqlQuery: `select _row_id, email_address, subject, body from "emails" where _row_id in (${affected_row_ids_key_list})`,
  }).response;

  const rows = unpackRows(response);
  console.log("xx - test");
  console.log("xx2", rows);

  if (rows == null) {
    return;
  }

  const RowT = t.type({
    _row_id: t.number,
    email_address: t.string,
    subject: t.string,
    body: t.string,
  });

  // for each row, run the logic
  for (const row of rows) {
    if (!RowT.is(row)) {
      continue;
    }
    const from: Mailbox = {
      email: fromEmail,
      name: fromName,
    };
    const to: Mailbox = {
      email: row.email_address,
    };

    const result = await sendEmail(
      sendgridApiKey,
      from,
      to,
      row.subject,
      row.body
    );

    const db = await getDbClient();

    await new MutationsBuilder()
      .updateRow("emails", row._row_id, {
        sent_timestamp: new Date().toISOString(),
      })
      .run(db);
  }
};

registerTransactionHandler(handler);

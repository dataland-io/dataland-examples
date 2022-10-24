import {
  registerTransactionHandler,
  getEnv,
  getHistoryClient,
  unpackRows,
  Transaction,
} from "@dataland-io/dataland-sdk";

const sendCustomerioEmail = async (id: string, name: string, email: string) => {
  const CUSTOMERIO_API_KEY = getEnv("CUSTOMERIO_API_KEY");

  const email_body =
    "Hi please reset your password here: https://example.com/reset-password";

  const body = {
    to: email,
    transactional_message_id: "2",
    identifiers: {
      id: id,
    },
    from: "parrot@redparrot.io",
    subject: "Password Reset",
    body: email_body,
  };

  const response = await fetch("https://track.customer.io/api/v1/email", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${CUSTOMERIO_API_KEY}`,
    },
    body: JSON.stringify(body),
  });

  if (response.status !== 200) {
    throw new Error("Error sending email to: " + email);");
  }

  console.log("Email sent to " + email);

  const result = await response.json();
  return result;
};

const handler = async (transaction: Transaction) => {
  const affected_row_ids = [];

  for (const mutation of transaction.mutations) {
    if (mutation.kind.oneofKind == "updateRows") {
      if (
        mutation.kind.updateRows.columnNames.includes("send_password_reset") &&
        mutation.kind.updateRows.tableName === "users"
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
  const response = await history.querySqlMirror({
    sqlQuery: `
    SELECT
      _row_id,
      id,
      name,
      email,
    FROM
      "users"
    WHERE
      _row_id in (${affected_row_ids_key_list})
    `,
  }).response;

  const rows = unpackRows(response);

  if (rows == null) {
    return;
  }

  // for each row, run the logic
  for (const row of rows) {
  }
};

registerTransactionHandler(handler);

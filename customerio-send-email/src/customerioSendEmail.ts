import {
  registerTransactionHandler,
  getEnv,
  getHistoryClient,
  unpackRows,
  Transaction,
  isString,
} from "@dataland-io/dataland-sdk";

// TODO: Reference the right parameters as arguments
const sendCustomerioEmail = async (id: string, name: string, email: string) => {
  const CUSTOMERIO_API_KEY = getEnv("CUSTOMERIO_API_KEY");

  const body = {
    to: email,
    // TODO: Update to the right template
    transactional_message_id: "3",
    message_data: {
      customer: {
        // TODO: Swap with the right parameters
        name: name,
        link: "https://example.com/reset-password/" + id,
        email: email,
      },
    },
    identifiers: {
      id: email,
    },

    // TODO: Swap with the right from email
    from: "parrot@redparrot.io",
  };

  const response = await fetch("https://api.customer.io/v1/send/email", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${CUSTOMERIO_API_KEY}`,
    },
    body: JSON.stringify(body),
  });

  console.log(response);

  if (response.status !== 200) {
    throw new Error("Error sending email to: " + email);
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
        // TODO: Update to the right table name and column name
        mutation.kind.updateRows.columnNames.includes("send_password_reset") &&
        mutation.kind.updateRows.tableName === "customer_io_users"
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

  const history = await getHistoryClient();
  // TODO: Supply the right parameters you need to populate the email with
  // TODO: Update to the right table name
  const response = await history.querySqlMirror({
    sqlQuery: `
    SELECT
      _row_id,
      id,
      name,
      email
    FROM
      "customer_io_users"
    WHERE
      _row_id in (${affected_row_ids_key_list})
    `,
  }).response;

  const rows = unpackRows(response);

  if (rows == null) {
    return;
  }

  for (const row of rows) {
    // TODO: Reference the right parameters
    if (!isString(row.id) || !isString(row.name) || !isString(row.email)) {
      continue;
    }
    await sendCustomerioEmail(row.id, row.name, row.email);
  }
};

registerTransactionHandler(handler);

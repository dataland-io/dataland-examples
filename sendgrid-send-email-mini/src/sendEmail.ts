import {
  getCatalogSnapshot,
  getEnv,
  Mutation,
  querySqlSnapshot,
  registerTransactionHandler,
  runMutations,
  Schema,
  Transaction,
  unpackRows,
} from "@dataland-io/dataland-sdk-worker";
import * as t from "io-ts";

interface Mailbox {
  email: string;
  name?: string;
}

// --------------------------------------------------------
// (awu): Define the sendEmail function
// --------------------------------------------------------
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
        type: "text/html",
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

// ------------------------------------------------------------
// (awu): Function is invoked by transactions in the Dataland.

// When users click a button in the Dataland, a transaction is
// created that invokes this function.
// ------------------------------------------------------------
const handler = async (transaction: Transaction) => {
  const sendgridApiKey = getEnv("SENDGRID_API_KEY");
  const fromEmail = "parrot@redparrot.io";
  const fromName = "Delivery Company - Demo";

  const { tableDescriptors } = await getCatalogSnapshot({
    logicalTimestamp: transaction.logicalTimestamp,
  });
  const schema = new Schema(tableDescriptors);

  const affectedRows = schema.getAffectedRows(
    "Orders Credit Workflow",
    "Send Email",
    transaction
  );

  const sendEmailKeys: number[] = [];
  for (const [key, value] of affectedRows) {
    if (typeof value === "number" && value > 0) {
      sendEmailKeys.push(key);
    }
  }

  if (sendEmailKeys.length === 0) {
    return;
  }

  const keyList = `(${sendEmailKeys.join(",")})`;

  // ---------------------------------------------------------------------------------------
  // (awu): Use Dataland SDK to read the state of the database and use in the email
  // ---------------------------------------------------------------------------------------
  const response = await querySqlSnapshot({
    logicalTimestamp: transaction.logicalTimestamp,
    sqlQuery: `
      select
        _dataland_key,
        "Email" as email_address,
        "Name" as name,
        "Order ID" as order_id
      from "Orders Credit Workflow"
      where _dataland_key in ${keyList}
    `,
  });

  const rows = unpackRows(response);

  const RowT = t.type({
    _dataland_key: t.number,
    email_address: t.string,
    name: t.string,
    order_id: t.number,
  });

  const mutations: Mutation[] = [];
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

    try {
      const sentTimestamp = new Date().toISOString();
      await sendEmail(
        sendgridApiKey,
        from,
        to,
        `Sorry about your order #${row.order_id}, ${row.name}`,
        `Hi ${row.name}, <br/><br/> Sorry about the delivery issues with your order #${row.order_id}. We gave you a $25 credit to make up for it. <br/> <br/> <br/>Best, <br/> Delivery Company`
      );
      console.log("Sent email to", row.email_address, "at", sentTimestamp);
      const update = schema.makeUpdateRows(
        "Orders Credit Workflow",
        row._dataland_key,
        {
          "Email sent at": sentTimestamp,
        }
      );
      mutations.push(update);
    } catch (e) {
      // continue to next
    }
  }

  if (mutations.length === 0) {
    return;
  }

  await runMutations({ mutations });
};

registerTransactionHandler(handler);

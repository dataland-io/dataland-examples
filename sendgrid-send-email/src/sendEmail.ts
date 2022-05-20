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
  const fromEmail = getEnv("FROM_EMAIL");
  const fromName = getEnv("FROM_NAME");

  const { tableDescriptors } = await getCatalogSnapshot({
    logicalTimestamp: transaction.logicalTimestamp,
  });
  const schema = new Schema(tableDescriptors);

  const affectedRows = schema.getAffectedRows(
    "emails",
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
  const response = await querySqlSnapshot({
    logicalTimestamp: transaction.logicalTimestamp,
    sqlQuery: `
      select
        _dataland_key,
        "Email Address" as email_address,
        Subject as subject,
        Body as body
      from emails
      where _dataland_key in ${keyList}
    `,
  });

  const rows = unpackRows(response);

  const RowT = t.type({
    _dataland_key: t.number,
    email_address: t.string,
    subject: t.string,
    body: t.string,
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
      await sendEmail(sendgridApiKey, from, to, row.subject, row.body);
      const sentTimestamp = new Date().toISOString();
      const update = schema.makeUpdateRows("emails", row._dataland_key, {
        "Sent Timestamp": sentTimestamp,
      });
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

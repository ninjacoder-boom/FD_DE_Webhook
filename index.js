require("dotenv").config();
const express = require("express");
const axios = require("axios");

const app = express();
app.use(express.json({ limit: "1mb" }));

const VERIFY_TOKEN = process.env.VERIFY_TOKEN || "myWebhookToken123";

const SFMC = {
  clientId:     process.env.SFMC_CLIENT_ID,
  clientSecret: process.env.SFMC_CLIENT_SECRET,
  subdomain:    process.env.SFMC_SUBDOMAIN,
  fdDeKey:      process.env.SFMC_DE_KEY         || "FD_DE",
  paymentDeKey: process.env.SFMC_PAYMENT_DE_KEY || "FD_Payment_DE",
  journeyEDK:   "APIEvent-cd97e0a4-80ef-2437-6681-1d44273aec2f"
};

// ─── ROI Map ──────────────────────────────────────────────────────────────────
const TENURE_ROI_MAP = {
  months12: 0.06,
  months24: 0.065,
  months36: 0.07,
  months48: 0.075
};

const TENURE_LABEL_MAP = {
  months12: "1 Year",
  months24: "2 Years",
  months36: "3 Years",
  months48: "4 Years"
};

function calculateMaturity(principal, tenure) {
  const roi = TENURE_ROI_MAP[tenure];
  if (!roi || !principal) return null;
  const years = parseInt(tenure.replace("months", ""), 10) / 12;
  const maturityAmount = Math.round(principal + principal * roi * years);
  const roiDisplay = String(parseFloat((roi * 100).toPrecision(10)));
  return { roiDisplay, maturityAmount };
}

// ─── Token Cache ──────────────────────────────────────────────────────────────
let tokenCache = { value: null, expiresAt: null };

async function getSFMCToken() {
  const now = Date.now();
  if (tokenCache.value && now < tokenCache.expiresAt - 60000) return tokenCache.value;

  const res = await axios.post(
    `https://${SFMC.subdomain}.auth.marketingcloudapis.com/v2/token`,
    {
      grant_type:    "client_credentials",
      client_id:     SFMC.clientId,
      client_secret: SFMC.clientSecret
    }
  );
  tokenCache.value     = res.data.access_token;
  tokenCache.expiresAt = now + res.data.expires_in * 1000;
  return tokenCache.value;
}

// ─── Save to FD_DE ────────────────────────────────────────────────────────────
async function saveToFDDE(data) {
  try {
    const token = await getSFMCToken();

    const payload = [
      {
        keys: { MessageId: data.messageId },
        values: {
          Subscriber_Key:      data.phone,
          PhoneNumber:         data.phone,
          ProfileName:         data.profileName        || "",
          HolderName:          data.holderName         || "",
          PANNumber:           data.panNumber          || "",
          Age:                 data.age                || "",
          Address:             data.address            || "",
          FD_Amount:           data.fdAmount           != null ? String(data.fdAmount) : "",
          Tenure:              data.tenure             || "",
          MaturityInstruction: data.maturityInstruction || "",
          NomineeName:         data.nomineeName        || "",
          NomineeRelation:     data.nomineeRelation    || "",
          ROI:                 data.roiDisplay         || "",
          Maturity_Amount:     data.maturityAmount     != null ? String(data.maturityAmount) : "",
          Timestamp:           data.timestamp
        }
      }
    ];

    await axios.post(
      `https://${SFMC.subdomain}.rest.marketingcloudapis.com/hub/v1/dataevents/key:${SFMC.fdDeKey}/rowset`,
      payload,
      { headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" } }
    );

    console.log(`[INFO] Saved to FD_DE | Phone: ${data.phone} | MsgID: ${data.messageId}`);
  } catch (err) {
    if (err.response?.status === 401) tokenCache = { value: null, expiresAt: null };
    console.error("[ERROR] Failed to save to FD_DE:", err.response?.data || err.message);
  }
}

// ─── Save to FD_Payment_DE ────────────────────────────────────────────────────
async function saveToPaymentDE(data) {
  try {
    const token = await getSFMCToken();

    const tenureLabel = TENURE_LABEL_MAP[data.tenure] || data.tenure || "";
    const paymentLink = `xyz`;

    const payload = [
      {
        keys: { ContactKey: data.phone },
        values: {
          MobileNo:    data.phone,
          PhoneNo:     data.phone,
          Name:        data.holderName || data.profileName || "",
          FDAmount:    data.fdAmount   != null ? String(data.fdAmount) : "",
          Tenure:      tenureLabel,
          PaymentLink: paymentLink,
          Status:      "Payment_Pending",
          Timestamp:   data.timestamp
        }
      }
    ];

    await axios.post(
      `https://${SFMC.subdomain}.rest.marketingcloudapis.com/hub/v1/dataevents/key:${SFMC.paymentDeKey}/rowset`,
      payload,
      { headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" } }
    );

    console.log(`[INFO] Saved to FD_Payment_DE | Phone: ${data.phone} | Tenure: ${tenureLabel} | Status: Payment_Pending`);
  } catch (err) {
    if (err.response?.status === 401) tokenCache = { value: null, expiresAt: null };
    console.error("[ERROR] Failed to save to FD_Payment_DE:", err.response?.data || err.message);
  }
}

// ─── Trigger SFMC Journey ─────────────────────────────────────────────────────
// async function triggerJourney(data) {
//   try {
//     const token = await getSFMCToken();

//     const tenureLabel = TENURE_LABEL_MAP[data.tenure] || data.tenure || "";
//     const paymentLink = `https://pay.link/${data.phone}_${data.messageId}`;

//     // ContactKey only at root — NOT duplicated inside Data
//     // const payload = {
//     //   ContactKey:         data.phone,
//     //   EventDefinitionKey: SFMC.journeyEDK,
//     //   Data: {
//     //     contactkey:  data.phone,        // ← lowercase, matches journey schema
//     //     MobileNo:    data.phone,
//     //     PhoneNo:     data.phone,
//     //     Name:        data.holderName || data.profileName || "",
//     //     FDAmount:    data.fdAmount   != null ? String(data.fdAmount) : "",
//     //     Tenure:      tenureLabel,
//     //     PaymentLink: paymentLink,
//     //     Status:      "Payment_Pending"
//     //   }
//     // };

//     const payload = {
//       ContactKey: `${data.phone}_${data.messageId}`, // unique per submission
//       EventDefinitionKey: SFMC.journeyEDK,
//       Data: {
//         contactkey: `${data.phone}_${data.messageId}`, // must match schema
//         MobileNo: data.phone,
//         PhoneNo: data.phone,
//         Name: data.holderName || data.profileName || "",
//         FDAmount: data.fdAmount != null ? String(data.fdAmount) : "",
//         Tenure: tenureLabel,
//         PaymentLink: paymentLink,
//         Status: "Payment_Pending"
//       }
//     };


//     console.log("[DEBUG] Journey payload:", JSON.stringify(payload, null, 2));

//     const response = await axios.post(
//       `https://${SFMC.subdomain}.rest.marketingcloudapis.com/interaction/v1/events`,
//       payload,
//       { headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" } }
//     );

//     console.log(`[INFO] Journey triggered successfully | EDK: ${SFMC.journeyEDK} | Phone: ${data.phone} | Response:`, response.data);
//   } catch (err) {
//     if (err.response?.status === 401) tokenCache = { value: null, expiresAt: null };
//     console.error("[ERROR] Failed to trigger Journey:", {
//       status:  err.response?.status,
//       data:    JSON.stringify(err.response?.data, null, 2),
//       message: err.message
//     });
//   }
// }


async function triggerJourney(data) {
  try {
    const token = await getSFMCToken();

    const tenureLabel = TENURE_LABEL_MAP[data.tenure] || data.tenure || "";
    const paymentLink = `xyz`;

    const uniqueKey = `${data.phone}_${data.messageId}`;

    const payload = {
      ContactKey: uniqueKey,
      EventDefinitionKey: SFMC.journeyEDK,
      Data: {
        contactkey: uniqueKey,
        MobileNo: data.phone,
        PhoneNo: data.phone,
        Name: data.holderName || data.profileName || "",
        FDAmount: data.fdAmount != null ? String(data.fdAmount) : "",
        Tenure: tenureLabel,
        PaymentLink: paymentLink,
        Status: "Payment_Pending"
      }
    };

    console.log("[DEBUG] Journey payload:", JSON.stringify(payload, null, 2));

    const response = await axios.post(
      `https://${SFMC.subdomain}.rest.marketingcloudapis.com/interaction/v1/events`,
      payload,
      { headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" } }
    );

    console.log(`[INFO] Journey triggered successfully | EDK: ${SFMC.journeyEDK} | Phone: ${data.phone} | Response:`, response.data);
  } catch (err) {
    if (err.response?.status === 401) tokenCache = { value: null, expiresAt: null };
    console.error("[ERROR] Failed to trigger Journey:", {
      status: err.response?.status,
      data: JSON.stringify(err.response?.data, null, 2),
      message: err.message
    });
  }
}



// ─── Webhook Verify ───────────────────────────────────────────────────────────
app.get("/webhook", (req, res) => {
  const { "hub.mode": mode, "hub.verify_token": token, "hub.challenge": challenge } = req.query;
  if (mode === "subscribe" && token === VERIFY_TOKEN) return res.status(200).send(challenge);
  return res.sendStatus(403);
});

// ─── Webhook POST ─────────────────────────────────────────────────────────────
app.post("/webhook", async (req, res) => {
  const body = req.body;

  if (body.object !== "whatsapp_business_account") {
    return res.status(404).json({ status: "error" });
  }

  const saved = [];

  for (const entry of body.entry || []) {
    for (const change of entry.changes || []) {
      const messages = change.value?.messages || [];
      const contacts = change.value?.contacts || [];

      for (const message of messages) {
        if (message.type !== "interactive" || message.interactive?.type !== "nfm_reply") continue;

        const flowResponse = message.interactive.nfm_reply?.response_json;
        if (!flowResponse) continue;

        let flowData;
        try {
          flowData = typeof flowResponse === "string" ? JSON.parse(flowResponse) : flowResponse;
        } catch {
          console.error("[ERROR] Failed to parse flow response_json");
          continue;
        }

        const phone       = message.from;
        const messageId   = message.id;
        const timestamp   = new Date(message.timestamp * 1000).toISOString();
        const contact     = contacts.find((c) => c.wa_id === phone);
        const profileName = contact?.profile?.name || "";

        const fdAmount = flowData.fd_amount ? parseFloat(flowData.fd_amount) : null;
        const tenure   = flowData.tenure    || null;

        let roiDisplay = null, maturityAmount = null;
        if (fdAmount && tenure) {
          const calc = calculateMaturity(fdAmount, tenure);
          if (calc) {
            roiDisplay     = calc.roiDisplay;
            maturityAmount = calc.maturityAmount;
          }
        }

        const record = {
          phone,
          messageId,
          timestamp,
          profileName,
          holderName:          flowData.holder_name          || "",
          panNumber:           flowData.pan_number           || "",
          age:                 flowData.age                  || "",
          address:             flowData.address              || "",
          fdAmount,
          tenure,
          maturityInstruction: flowData.maturity_instruction || "",
          nomineeName:         flowData.nominee_name         || "",
          nomineeRelation:     flowData.nominee_relation     || "",
          roiDisplay,
          maturityAmount
        };

        // ── Step 1: Save to FD_DE ─────────────────────────────────────────────
        await saveToFDDE(record);
        console.log(`[INFO] Flow captured | Phone: ${phone} | FD: ${fdAmount} | Tenure: ${tenure} | ROI: ${roiDisplay}% | Maturity: ${maturityAmount}`);

        // ── Step 2: Save to FD_Payment_DE ─────────────────────────────────────
        await saveToPaymentDE(record);

        // ── Step 3: Trigger SFMC Journey ──────────────────────────────────────
        await triggerJourney(record);

        saved.push(record);
      }
    }
  }

  return res.status(200).json({ status: "ok", received: saved.length });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`[INFO] Server running on port ${PORT}`));







// require("dotenv").config();
// const express = require("express");
// const axios = require("axios");

// const app = express();
// app.use(express.json({ limit: "1mb" }));

// const VERIFY_TOKEN = process.env.VERIFY_TOKEN || "myWebhookToken123";

// const SFMC = {
//   clientId:     process.env.SFMC_CLIENT_ID,
//   clientSecret: process.env.SFMC_CLIENT_SECRET,
//   subdomain:    process.env.SFMC_SUBDOMAIN,
//   fdDeKey:      process.env.SFMC_DE_KEY         || "FD_DE",
//   paymentDeKey: process.env.SFMC_PAYMENT_DE_KEY || "FD_Payment_DE",
//   journeyEDK:   "APIEvent-cd97e0a4-80ef-2437-6681-1d44273aec2f"
// };

// // ─── ROI Map ──────────────────────────────────────────────────────────────────
// const TENURE_ROI_MAP = {
//   months12: 0.06,
//   months24: 0.065,
//   months36: 0.07,
//   months48: 0.075
// };

// const TENURE_LABEL_MAP = {
//   months12: "1 Year",
//   months24: "2 Years",
//   months36: "3 Years",
//   months48: "4 Years"
// };

// function calculateMaturity(principal, tenure) {
//   const roi = TENURE_ROI_MAP[tenure];
//   if (!roi || !principal) return null;
//   const years = parseInt(tenure.replace("months", ""), 10) / 12;
//   const maturityAmount = Math.round(principal + principal * roi * years);
//   const roiDisplay = String(parseFloat((roi * 100).toPrecision(10)));
//   return { roiDisplay, maturityAmount };
// }

// // ─── Token Cache ──────────────────────────────────────────────────────────────
// let tokenCache = { value: null, expiresAt: null };

// async function getSFMCToken() {
//   const now = Date.now();
//   if (tokenCache.value && now < tokenCache.expiresAt - 60000) return tokenCache.value;

//   const res = await axios.post(
//     `https://${SFMC.subdomain}.auth.marketingcloudapis.com/v2/token`,
//     {
//       grant_type:    "client_credentials",
//       client_id:     SFMC.clientId,
//       client_secret: SFMC.clientSecret
//     }
//   );
//   tokenCache.value     = res.data.access_token;
//   tokenCache.expiresAt = now + res.data.expires_in * 1000;
//   return tokenCache.value;
// }

// // ─── Save to FD_DE ────────────────────────────────────────────────────────────
// async function saveToFDDE(data) {
//   try {
//     const token = await getSFMCToken();

//     const payload = [
//       {
//         keys: { MessageId: data.messageId },
//         values: {
//           Subscriber_Key:      data.phone,
//           PhoneNumber:         data.phone,
//           ProfileName:         data.profileName        || "",
//           HolderName:          data.holderName         || "",
//           PANNumber:           data.panNumber          || "",
//           Age:                 data.age                || "",
//           Address:             data.address            || "",
//           FD_Amount:           data.fdAmount           != null ? String(data.fdAmount) : "",
//           Tenure:              data.tenure             || "",
//           MaturityInstruction: data.maturityInstruction || "",
//           NomineeName:         data.nomineeName        || "",
//           NomineeRelation:     data.nomineeRelation    || "",
//           ROI:                 data.roiDisplay         || "",
//           Maturity_Amount:     data.maturityAmount     != null ? String(data.maturityAmount) : "",
//           Timestamp:           data.timestamp
//         }
//       }
//     ];

//     await axios.post(
//       `https://${SFMC.subdomain}.rest.marketingcloudapis.com/hub/v1/dataevents/key:${SFMC.fdDeKey}/rowset`,
//       payload,
//       { headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" } }
//     );

//     console.log(`[INFO] Saved to FD_DE | Phone: ${data.phone} | MsgID: ${data.messageId}`);
//   } catch (err) {
//     if (err.response?.status === 401) tokenCache = { value: null, expiresAt: null };
//     console.error("[ERROR] Failed to save to FD_DE:", err.response?.data || err.message);
//   }
// }

// // ─── Save to FD_Payment_DE ────────────────────────────────────────────────────
// async function saveToPaymentDE(data) {
//   try {
//     const token = await getSFMCToken();

//     const tenureLabel = TENURE_LABEL_MAP[data.tenure] || data.tenure || "";
//     const paymentLink = `https://pay.link/${data.phone}_${data.messageId}`;

//     const payload = [
//       {
//         keys: { ContactKey: data.phone },
//         values: {
//           MobileNo:    data.phone,
//           PhoneNo:     data.phone,
//           Name:        data.holderName || data.profileName || "",
//           FDAmount:    data.fdAmount   != null ? String(data.fdAmount) : "",
//           Tenure:      tenureLabel,
//           PaymentLink: paymentLink,
//           Status:      "Payment_Pending",
//           Timestamp:   data.timestamp
//         }
//       }
//     ];

//     await axios.post(
//       `https://${SFMC.subdomain}.rest.marketingcloudapis.com/hub/v1/dataevents/key:${SFMC.paymentDeKey}/rowset`,
//       payload,
//       { headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" } }
//     );

//     console.log(`[INFO] Saved to FD_Payment_DE | Phone: ${data.phone} | Tenure: ${tenureLabel} | Status: Payment_Pending`);
//   } catch (err) {
//     if (err.response?.status === 401) tokenCache = { value: null, expiresAt: null };
//     console.error("[ERROR] Failed to save to FD_Payment_DE:", err.response?.data || err.message);
//   }
// }

// ─── Trigger SFMC Journey ─────────────────────────────────────────────────────
// async function triggerJourney(data) {
//   try {
//     const token = await getSFMCToken();

//     const tenureLabel = TENURE_LABEL_MAP[data.tenure] || data.tenure || "";
//     const paymentLink = `https://pay.link/${data.phone}_${data.messageId}`;

//     // ContactKey only at root — NOT duplicated inside Data
//     // const payload = {
//     //   ContactKey:         data.phone,
//     //   EventDefinitionKey: SFMC.journeyEDK,
//     //   Data: {
//     //     contactkey:  data.phone,        // ← lowercase, matches journey schema
//     //     MobileNo:    data.phone,
//     //     PhoneNo:     data.phone,
//     //     Name:        data.holderName || data.profileName || "",
//     //     FDAmount:    data.fdAmount   != null ? String(data.fdAmount) : "",
//     //     Tenure:      tenureLabel,
//     //     PaymentLink: paymentLink,
//     //     Status:      "Payment_Pending"
//     //   }
//     // };

//     const payload = {
//       ContactKey: `${data.phone}_${data.messageId}`, // unique per submission
//       EventDefinitionKey: SFMC.journeyEDK,
//       Data: {
//         contactkey: `${data.phone}_${data.messageId}`, // must match schema
//         MobileNo: data.phone,
//         PhoneNo: data.phone,
//         Name: data.holderName || data.profileName || "",
//         FDAmount: data.fdAmount != null ? String(data.fdAmount) : "",
//         Tenure: tenureLabel,
//         PaymentLink: paymentLink,
//         Status: "Payment_Pending"
//       }
//     };


//     console.log("[DEBUG] Journey payload:", JSON.stringify(payload, null, 2));

//     const response = await axios.post(
//       `https://${SFMC.subdomain}.rest.marketingcloudapis.com/interaction/v1/events`,
//       payload,
//       { headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" } }
//     );

//     console.log(`[INFO] Journey triggered successfully | EDK: ${SFMC.journeyEDK} | Phone: ${data.phone} | Response:`, response.data);
//   } catch (err) {
//     if (err.response?.status === 401) tokenCache = { value: null, expiresAt: null };
//     console.error("[ERROR] Failed to trigger Journey:", {
//       status:  err.response?.status,
//       data:    JSON.stringify(err.response?.data, null, 2),
//       message: err.message
//     });
//   }
// }


async function triggerJourney(data) {
  try {
    const token = await getSFMCToken();

    const tenureLabel = TENURE_LABEL_MAP[data.tenure] || data.tenure || "";
    const paymentLink = `https://pay.link/${data.phone}_${data.messageId}`;

    const uniqueKey = `${data.phone}_${data.messageId}`;

    const payload = {
      ContactKey: uniqueKey,
      EventDefinitionKey: SFMC.journeyEDK,
      Data: {
        contactkey: uniqueKey,
        MobileNo: data.phone,
        PhoneNo: data.phone,
        Name: data.holderName || data.profileName || "",
        FDAmount: data.fdAmount != null ? String(data.fdAmount) : "",
        Tenure: tenureLabel,
        PaymentLink: paymentLink,
        Status: "Payment_Pending"
      }
    };

    console.log("[DEBUG] Journey payload:", JSON.stringify(payload, null, 2));

    const response = await axios.post(
      `https://${SFMC.subdomain}.rest.marketingcloudapis.com/interaction/v1/events`,
      payload,
      { headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" } }
    );

    console.log(`[INFO] Journey triggered successfully | EDK: ${SFMC.journeyEDK} | Phone: ${data.phone} | Response:`, response.data);
  } catch (err) {
    if (err.response?.status === 401) tokenCache = { value: null, expiresAt: null };
    console.error("[ERROR] Failed to trigger Journey:", {
      status: err.response?.status,
      data: JSON.stringify(err.response?.data, null, 2),
      message: err.message
    });
  }
}



// ─── Webhook Verify ───────────────────────────────────────────────────────────
app.get("/webhook", (req, res) => {
  const { "hub.mode": mode, "hub.verify_token": token, "hub.challenge": challenge } = req.query;
  if (mode === "subscribe" && token === VERIFY_TOKEN) return res.status(200).send(challenge);
  return res.sendStatus(403);
});

// ─── Webhook POST ─────────────────────────────────────────────────────────────
app.post("/webhook", async (req, res) => {
  const body = req.body;

  if (body.object !== "whatsapp_business_account") {
    return res.status(404).json({ status: "error" });
  }

  const saved = [];

  for (const entry of body.entry || []) {
    for (const change of entry.changes || []) {
      const messages = change.value?.messages || [];
      const contacts = change.value?.contacts || [];

      for (const message of messages) {
        if (message.type !== "interactive" || message.interactive?.type !== "nfm_reply") continue;

        const flowResponse = message.interactive.nfm_reply?.response_json;
        if (!flowResponse) continue;

        let flowData;
        try {
          flowData = typeof flowResponse === "string" ? JSON.parse(flowResponse) : flowResponse;
        } catch {
          console.error("[ERROR] Failed to parse flow response_json");
          continue;
        }

        const phone       = message.from;
        const messageId   = message.id;
        const timestamp   = new Date(message.timestamp * 1000).toISOString();
        const contact     = contacts.find((c) => c.wa_id === phone);
        const profileName = contact?.profile?.name || "";

        const fdAmount = flowData.fd_amount ? parseFloat(flowData.fd_amount) : null;
        const tenure   = flowData.tenure    || null;

        let roiDisplay = null, maturityAmount = null;
        if (fdAmount && tenure) {
          const calc = calculateMaturity(fdAmount, tenure);
          if (calc) {
            roiDisplay     = calc.roiDisplay;
            maturityAmount = calc.maturityAmount;
          }
        }

        const record = {
          phone,
          messageId,
          timestamp,
          profileName,
          holderName:          flowData.holder_name          || "",
          panNumber:           flowData.pan_number           || "",
          age:                 flowData.age                  || "",
          address:             flowData.address              || "",
          fdAmount,
          tenure,
          maturityInstruction: flowData.maturity_instruction || "",
          nomineeName:         flowData.nominee_name         || "",
          nomineeRelation:     flowData.nominee_relation     || "",
          roiDisplay,
          maturityAmount
        };

        // ── Step 1: Save to FD_DE ─────────────────────────────────────────────
        await saveToFDDE(record);
        console.log(`[INFO] Flow captured | Phone: ${phone} | FD: ${fdAmount} | Tenure: ${tenure} | ROI: ${roiDisplay}% | Maturity: ${maturityAmount}`);

        // ── Step 2: Save to FD_Payment_DE ─────────────────────────────────────
        await saveToPaymentDE(record);

        // ── Step 3: Trigger SFMC Journey ──────────────────────────────────────
        await triggerJourney(record);

        saved.push(record);
      }
    }
  }

  return res.status(200).json({ status: "ok", received: saved.length });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`[INFO] Server running on port ${PORT}`));










// require("dotenv").config();
// const express = require("express");
// const axios = require("axios");

// const app = express();
// app.use(express.json({ limit: "1mb" }));

// const VERIFY_TOKEN = process.env.VERIFY_TOKEN || "myWebhookToken123";

// const SFMC = {
//   clientId:     process.env.SFMC_CLIENT_ID,
//   clientSecret: process.env.SFMC_CLIENT_SECRET,
//   subdomain:    process.env.SFMC_SUBDOMAIN,
//   fdDeKey:      "FD_DE"   // ← your new DE external key
// };

// // ─── ROI Map ──────────────────────────────────────────────────────────────────
// const TENURE_ROI_MAP = {
//   months12: 0.06,
//   months24: 0.065,
//   months36: 0.07,
//   months48: 0.075
// };

// function calculateMaturity(principal, tenure) {
//   const roi = TENURE_ROI_MAP[tenure];
//   if (!roi || !principal) return null;
//   const years = parseInt(tenure.replace("months", ""), 10) / 12;
//   const maturityAmount = Math.round(principal + principal * roi * years);
//   const roiDisplay = String(parseFloat((roi * 100).toPrecision(10)));
//   return { roiDisplay, maturityAmount };
// }

// // ─── Token Cache ──────────────────────────────────────────────────────────────
// let tokenCache = { value: null, expiresAt: null };

// async function getSFMCToken() {
//   const now = Date.now();
//   if (tokenCache.value && now < tokenCache.expiresAt - 60000) return tokenCache.value;

//   const res = await axios.post(
//     `https://${SFMC.subdomain}.auth.marketingcloudapis.com/v2/token`,
//     {
//       grant_type:    "client_credentials",
//       client_id:     SFMC.clientId,
//       client_secret: SFMC.clientSecret
//     }
//   );
//   tokenCache.value     = res.data.access_token;
//   tokenCache.expiresAt = now + res.data.expires_in * 1000;
//   return tokenCache.value;
// }

// // ─── Save to FD_DE ────────────────────────────────────────────────────────────
// async function saveToFDDE(data) {
//   try {
//     const token = await getSFMCToken();

//     const payload = [
//       {
//         keys: { MessageId: data.messageId },
//         values: {
//           Subscriber_Key:      data.phone,
//           PhoneNumber:         data.phone,
//           ProfileName:         data.profileName       || "",
//           HolderName:          data.holderName         || "",
//           PANNumber:           data.panNumber          || "",
//           Age:                 data.age                || "",
//           Address:             data.address            || "",
//           FD_Amount:           data.fdAmount           != null ? String(data.fdAmount)       : "",
//           Tenure:              data.tenure             || "",
//           MaturityInstruction: data.maturityInstruction || "",
//           NomineeName:         data.nomineeName        || "",
//           NomineeRelation:     data.nomineeRelation    || "",
//           ROI:                 data.roiDisplay         || "",
//           Maturity_Amount:     data.maturityAmount     != null ? String(data.maturityAmount) : "",
//           Timestamp:           data.timestamp
//         }
//       }
//     ];

//     await axios.post(
//       `https://${SFMC.subdomain}.rest.marketingcloudapis.com/hub/v1/dataevents/key:${SFMC.fdDeKey}/rowset`,
//       payload,
//       { headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" } }
//     );

//     console.log(`[INFO] Saved to FD_DE | Phone: ${data.phone} | MsgID: ${data.messageId}`);
//   } catch (err) {
//     if (err.response?.status === 401) tokenCache = { value: null, expiresAt: null };
//     console.error("[ERROR] Failed to save to FD_DE:", err.response?.data || err.message);
//   }
// }

// // ─── Webhook Verify ───────────────────────────────────────────────────────────
// app.get("/webhook", (req, res) => {
//   const { "hub.mode": mode, "hub.verify_token": token, "hub.challenge": challenge } = req.query;
//   if (mode === "subscribe" && token === VERIFY_TOKEN) return res.status(200).send(challenge);
//   return res.sendStatus(403);
// });

// // ─── Webhook POST ─────────────────────────────────────────────────────────────
// app.post("/webhook", async (req, res) => {
//   const body = req.body;

//   if (body.object !== "whatsapp_business_account") {
//     return res.status(404).json({ status: "error" });
//   }

//   const saved = [];

//   for (const entry of body.entry || []) {
//     for (const change of entry.changes || []) {
//       const messages = change.value?.messages || [];
//       const contacts = change.value?.contacts || [];

//       for (const message of messages) {
//         // Only handle Flow (nfm_reply) messages
//         if (message.type !== "interactive" || message.interactive?.type !== "nfm_reply") continue;

//         const flowResponse = message.interactive.nfm_reply?.response_json;
//         if (!flowResponse) continue;

//         let flowData;
//         try {
//           flowData = typeof flowResponse === "string" ? JSON.parse(flowResponse) : flowResponse;
//         } catch {
//           console.error("[ERROR] Failed to parse flow response_json");
//           continue;
//         }

//         const phone     = message.from;
//         const messageId = message.id;
//         const timestamp = new Date(message.timestamp * 1000).toISOString();
//         const contact   = contacts.find((c) => c.wa_id === phone);
//         const profileName = contact?.profile?.name || "";

//         // Extract all flow fields
//         const fdAmount  = flowData.fd_amount ? parseFloat(flowData.fd_amount) : null;
//         const tenure    = flowData.tenure    || null;

//         // Calculate ROI + maturity
//         let roiDisplay = null, maturityAmount = null;
//         if (fdAmount && tenure) {
//           const calc = calculateMaturity(fdAmount, tenure);
//           if (calc) {
//             roiDisplay     = calc.roiDisplay;
//             maturityAmount = calc.maturityAmount;
//           }
//         }

//         const record = {
//           phone,
//           messageId,
//           timestamp,
//           profileName,
//           holderName:          flowData.holder_name          || "",
//           panNumber:           flowData.pan_number           || "",
//           age:                 flowData.age                  || "",
//           address:             flowData.address              || "",
//           fdAmount,
//           tenure,
//           maturityInstruction: flowData.maturity_instruction || "",
//           nomineeName:         flowData.nominee_name         || "",
//           nomineeRelation:     flowData.nominee_relation     || "",
//           roiDisplay,
//           maturityAmount
//         };

//         // Save to FD_DE (non-blocking)
//         saveToFDDE(record);
//         saved.push(record);

//         console.log(`[INFO] Flow captured | Phone: ${phone} | FD: ${fdAmount} | Tenure: ${tenure} | ROI: ${roiDisplay}% | Maturity: ${maturityAmount}`);
//       }
//     }
//   }

//   return res.status(200).json({ status: "ok", received: saved.length });
// });

// const PORT = process.env.PORT || 3000;
// app.listen(PORT, () => console.log(`[INFO] Server running on port ${PORT}`));

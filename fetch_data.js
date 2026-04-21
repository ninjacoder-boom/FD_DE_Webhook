require("dotenv").config();
const express = require("express");
const axios = require("axios");

const app = express();
app.use(express.json({ limit: "1mb" }));

const VERIFY_TOKEN = process.env.VERIFY_TOKEN || "myWebhookToken123";

const SFMC = {
  clientId: process.env.SFMC_CLIENT_ID,
  clientSecret: process.env.SFMC_CLIENT_SECRET,
  subdomain: process.env.SFMC_SUBDOMAIN,
  deExternalKey: process.env.SFMC_DE_KEY || "FD_Flow",
  fdAmountDeKey: process.env.SFMC_FD_AMOUNT_DE_KEY || "Internal_bhav"
};

const isDev = process.env.NODE_ENV !== "production";

function log(level, message, meta) {
  if (!isDev && level === "DEBUG") return;
  const timestamp = new Date().toISOString();
  let metaStr = "";
  if (meta !== undefined && meta !== null && meta !== "") {
    metaStr = typeof meta === "object" ? JSON.stringify(meta, null, 2) : String(meta);
  }
  console.log(`[${timestamp}] [${level}] ${message}${metaStr ? " " + metaStr : ""}`);
}

let tokenCache = { value: null, expiresAt: null };

async function getSFMCToken() {
  const now = Date.now();
  if (tokenCache.value && now < tokenCache.expiresAt - 60000) {
    return tokenCache.value;
  }
  try {
    const response = await axios.post(
      `https://${SFMC.subdomain}.auth.marketingcloudapis.com/v2/token`,
      {
        grant_type: "client_credentials",
        client_id: SFMC.clientId,
        client_secret: SFMC.clientSecret
      }
    );
    tokenCache.value = response.data.access_token;
    tokenCache.expiresAt = now + response.data.expires_in * 1000;
    log("INFO", "New SFMC token fetched");
    return tokenCache.value;
  } catch (err) {
    log("ERROR", "SFMC token fetch failed:", err.response?.data || err.message);
    throw err;
  }
}

/**
 * Fetch FD_Amount from Internal_bhav DE by Subscriber_Key (phone number).
 * SFMC returns fields split across item.keys and item.values — both lowercase.
 * Merge them before reading field values.
 */
async function fetchFDAmountFromDE(phone) {
  try {
    const token = await getSFMCToken();

    const url = `https://${SFMC.subdomain}.rest.marketingcloudapis.com/data/v1/customobjectdata/key/${SFMC.fdAmountDeKey}/rowset?$filter=Subscriber_Key%20eq%20'${phone}'`;

    log("INFO", `Fetching FD record for Subscriber_Key: ${phone}`);

    const response = await axios.get(url, {
      headers: { Authorization: `Bearer ${token}` }
    });

    const items = response.data?.items || [];

    if (items.length === 0) {
      log("WARN", `No record found in ${SFMC.fdAmountDeKey} for Subscriber_Key: ${phone}`);
      return null;
    }

    // SFMC splits primary keys into item.keys and other fields into item.values
    // Both use all-lowercase field names — merge before reading
    const merged = {
      ...(items[0].keys   || {}),
      ...(items[0].values || {})
    };

    const amount = merged.fd_amount ?? null;
    log("INFO", `FD_Amount for ${phone}: ${amount}`);
    return amount;

  } catch (err) {
    if (err.response?.status === 401) {
      tokenCache = { value: null, expiresAt: null };
    }
    log("ERROR", `Failed to fetch FD record for ${phone}:`, {
      status: err.response?.status,
      data: err.response?.data,
      message: err.message
    });
    return null;
  }
}

async function saveFlowDataToDE({
  from, profileName, amountType, partialAmount,
  finalAmount, tenure, messageId, timestamp
}) {
  try {
    const token = await getSFMCToken();

    const payload = [
      {
        keys: { MessageId: messageId },
        values: {
          PhoneNumber: from,
          ProfileName: profileName,
          AmountType: amountType,
          PartialAmount: partialAmount,
          FinalAmount: finalAmount,
          Tenure: tenure,
          MessageId: messageId,
          Timestamp: timestamp
        }
      }
    ];

    await axios.post(
      `https://${SFMC.subdomain}.rest.marketingcloudapis.com/hub/v1/dataevents/key:${SFMC.deExternalKey}/rowset`,
      payload,
      { headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" } }
    );

    log("INFO", `Saved to DE | Phone: ${from} | Name: ${profileName} | AmountType: ${amountType} | PartialAmount: ${partialAmount} | FinalAmount: ${finalAmount} | Tenure: ${tenure} | MsgID: ${messageId} | Time: ${timestamp}`);
  } catch (err) {
    if (err.response?.status === 401) {
      tokenCache = { value: null, expiresAt: null };
    }
    log("ERROR", "Failed to save Flow data to DE:", {
      status: err.response?.status,
      data: err.response?.data,
      message: err.message
    });
  }
}

// ─── Diagnostic: GET /test-fetch/:phone ──────────────────────────────────────
// Returns the single matched record for a phone number from Internal_bhav DE
// e.g. GET http://localhost:3000/test-fetch/919149074149
app.get("/test-fetch/:phone", async (req, res) => {
  const phone = req.params.phone;
  log("INFO", `[TEST] Fetch triggered for Subscriber_Key: ${phone}`);

  try {
    const token = await getSFMCToken();

    const url = `https://${SFMC.subdomain}.rest.marketingcloudapis.com/data/v1/customobjectdata/key/${SFMC.fdAmountDeKey}/rowset?$filter=Subscriber_Key%20eq%20'${phone}'`;

    const response = await axios.get(url, {
      headers: { Authorization: `Bearer ${token}` }
    });

    const items = response.data?.items || [];

    if (items.length === 0) {
      log("WARN", `[TEST] No record found for Subscriber_Key: ${phone}`);
      return res.status(404).json({ success: false, message: `No record found for Subscriber_Key: ${phone}` });
    }

    // Merge keys + values (SFMC returns all field names lowercase)
    const merged = {
      ...(items[0].keys   || {}),
      ...(items[0].values || {})
    };

    const record = {
      Subscriber_Key:  merged.subscriber_key  ?? null,
      Mobile:          merged.mobile          ?? null,
      Name:            merged.name            ?? null,
      Pan_Number:      merged.pan_number      ?? null,
      FD_Amount:       merged.fd_amount       ?? null,
      Nominee_Name:    merged.nominee_name    ?? null,
      Tenure:          merged.tenure          ?? null,
      Locale:          merged.locale          ?? null,
      ROI:             merged.roi             ?? null,
      Maturity_Amount: merged.maturity_amount ?? null,
      FDR_NO:          merged.fdr_no          ?? null
    };

    log("INFO", `[TEST] Record found for ${phone}:`, record);
    return res.status(200).json({ success: true, data: record });

  } catch (err) {
    log("ERROR", "[TEST] Fetch failed:", err.response?.data || err.message);
    return res.status(500).json({ success: false, error: err.response?.data || err.message });
  }
});
// ─────────────────────────────────────────────────────────────────────────────

// ─── WhatsApp webhook verify ──────────────────────────────────────────────────
app.get("/webhook", (req, res) => {
  const mode      = req.query["hub.mode"];
  const token     = req.query["hub.verify_token"];
  const challenge = req.query["hub.challenge"];

  if (mode === "subscribe" && token === VERIFY_TOKEN) {
    log("INFO", "Webhook verified");
    return res.status(200).send(challenge);
  }
  return res.sendStatus(403);
});

// ─── WhatsApp webhook POST ────────────────────────────────────────────────────
app.post("/webhook", async (req, res) => {
  const body = req.body;

  if (body.object !== "whatsapp_business_account") {
    return res.status(404).json({ status: "error", message: "Not a whatsapp_business_account event" });
  }

  const collectedFlowData = [];

  try {
    for (const entry of body.entry || []) {
      for (const change of entry.changes || []) {
        const messages = change.value?.messages || [];
        const contacts = change.value?.contacts || [];

        for (const message of messages) {
          const from        = message.from;
          const messageId   = message.id;
          const timestamp   = new Date(message.timestamp * 1000).toISOString();
          const contact     = contacts.find((c) => c.wa_id === from);
          const profileName = contact?.profile?.name || "";

          if (message.type === "interactive" && message.interactive?.type === "nfm_reply") {
            const flowResponse = message.interactive.nfm_reply?.response_json;
            if (!flowResponse) {
              log("DEBUG", "No response_json in flow reply, skipping");
              continue;
            }

            let flowData;
            try {
              flowData = typeof flowResponse === "string" ? JSON.parse(flowResponse) : flowResponse;
            } catch {
              log("ERROR", "Failed to parse flow response_json");
              continue;
            }

            const amountType    = flowData?.amount_type    ?? null;
            const partialAmount = flowData?.partial_amount ?? null;
            const tenure        = flowData?.tenure         ?? null;

            if (amountType === null && tenure === null) {
              log("DEBUG", "Flow response missing amount_type and tenure, skipping");
              continue;
            }

            let finalAmount = null;

            if (amountType === "full") {
              log("INFO", `amount_type=full — fetching FD_Amount for Subscriber_Key: ${from}`);
              finalAmount = await fetchFDAmountFromDE(from);
            } else {
              finalAmount = partialAmount;
            }

            log("INFO", `Flow Data → Phone: ${from} | Name: ${profileName} | AmountType: ${amountType} | PartialAmount: ${partialAmount} | FinalAmount: ${finalAmount} | Tenure: ${tenure} | MsgID: ${messageId} | Time: ${timestamp}`);

            collectedFlowData.push({ phone: from, profileName, amountType, partialAmount, finalAmount, tenure, messageId, timestamp });

            saveFlowDataToDE({ from, profileName, amountType, partialAmount, finalAmount, tenure, messageId, timestamp });
          }
        }
      }
    }
  } catch (err) {
    log("ERROR", "Webhook processing failed:", err.message);
    return res.status(500).json({ status: "error", message: err.message });
  }

  return res.status(200).json({ status: "ok", received: collectedFlowData.length, flowData: collectedFlowData });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => log("INFO", `Server running on port ${PORT}`));











// require("dotenv").config();
// const express = require("express");
// const axios = require("axios");

// const app = express();
// app.use(express.json({ limit: "1mb" }));

// const VERIFY_TOKEN = process.env.VERIFY_TOKEN || "myWebhookToken123";

// const SFMC = {
//   clientId: process.env.SFMC_CLIENT_ID,
//   clientSecret: process.env.SFMC_CLIENT_SECRET,
//   subdomain: process.env.SFMC_SUBDOMAIN,
//   deExternalKey: process.env.SFMC_DE_KEY || "FD_Flow",
//   fdAmountDeKey: process.env.SFMC_FD_AMOUNT_DE_KEY || "Internal_bhav"
// };

// const isDev = process.env.NODE_ENV !== "production";

// function log(level, message, meta) {
//   if (!isDev && level === "DEBUG") return;
//   const timestamp = new Date().toISOString();
//   let metaStr = "";
//   if (meta !== undefined && meta !== null && meta !== "") {
//     metaStr = typeof meta === "object" ? JSON.stringify(meta, null, 2) : String(meta);
//   }
//   console.log(`[${timestamp}] [${level}] ${message}${metaStr ? " " + metaStr : ""}`);
// }

// let tokenCache = { value: null, expiresAt: null };

// async function getSFMCToken() {
//   const now = Date.now();
//   if (tokenCache.value && now < tokenCache.expiresAt - 60000) {
//     return tokenCache.value;
//   }
//   try {
//     const response = await axios.post(
//       `https://${SFMC.subdomain}.auth.marketingcloudapis.com/v2/token`,
//       {
//         grant_type: "client_credentials",
//         client_id: SFMC.clientId,
//         client_secret: SFMC.clientSecret
//       }
//     );
//     tokenCache.value = response.data.access_token;
//     tokenCache.expiresAt = now + response.data.expires_in * 1000;
//     log("INFO", "New SFMC token fetched");
//     return tokenCache.value;
//   } catch (err) {
//     log("ERROR", "SFMC token fetch failed:", err.response?.data || err.message);
//     throw err;
//   }
// }

// /**
//  * Fetch FD_Amount from Internal_bhav DE.
//  * DE Primary Keys: Subscriber_Key + Mobile (both = phone number)
//  * Fields: Subscriber_Key, Mobile, Name, Pan_Number, FD_Amount,
//  *         Nominee_Name, Tenure, Locale, ROI, Maturity_Amount, FDR_NO
//  *
//  * Uses REST: GET /data/v1/customobjectdata/key/{externalKey}/rowset
//  * with OData $filter on Subscriber_Key (primary key)
//  */
// async function fetchFDAmountFromDE(phone) {
//   try {
//     const token = await getSFMCToken();

//     // Primary key lookup via OData filter on the correct read endpoint
//     const url = `https://${SFMC.subdomain}.rest.marketingcloudapis.com/data/v1/customobjectdata/key/${SFMC.fdAmountDeKey}/rowset?$filter=Subscriber_Key%20eq%20'${phone}'`;

//     log("INFO", `Fetching FD_Amount | URL: ${url}`);

//     const response = await axios.get(url, {
//       headers: {
//         Authorization: `Bearer ${token}`,
//         "Content-Type": "application/json"
//       }
//     });

//     // Always log full raw response so we can see exact field names SFMC returns
//     log("INFO", `Internal_bhav raw response for ${phone}:`, response.data);

//     const items = response.data?.items || [];

//     if (items.length === 0) {
//       log("WARN", `No record found in ${SFMC.fdAmountDeKey} for Subscriber_Key: ${phone}`);
//       return null;
//     }

//     const values = items[0]?.values || {};

//     // Log all returned field names so we can confirm exact casing
//     log("INFO", `Returned field keys for ${phone}:`, Object.keys(values));

//     // DE field name is FD_Amount (from screenshot) — try all casing variants
//     const amount =
//       values?.FD_Amount ??
//       values?.fd_amount ??
//       values?.Fd_Amount ??
//       values?.["FD_Amount"] ??
//       null;

//     log("INFO", `Fetched FD_Amount for ${phone}: ${amount}`);
//     return amount;
//   } catch (err) {
//     if (err.response?.status === 401) {
//       tokenCache = { value: null, expiresAt: null };
//     }
//     log("ERROR", `Failed to fetch FD_Amount for ${phone}:`, {
//       status: err.response?.status,
//       data: err.response?.data,
//       message: err.message
//     });
//     return null;
//   }
// }

// async function saveFlowDataToDE({
//   from,
//   profileName,
//   amountType,
//   partialAmount,
//   finalAmount,
//   tenure,
//   messageId,
//   timestamp
// }) {
//   try {
//     const token = await getSFMCToken();

//     const payload = [
//       {
//         keys: { MessageId: messageId },
//         values: {
//           PhoneNumber: from,
//           ProfileName: profileName,
//           AmountType: amountType,
//           PartialAmount: partialAmount,
//           FinalAmount: finalAmount,
//           Tenure: tenure,
//           MessageId: messageId,
//           Timestamp: timestamp
//         }
//       }
//     ];

//     await axios.post(
//       `https://${SFMC.subdomain}.rest.marketingcloudapis.com/hub/v1/dataevents/key:${SFMC.deExternalKey}/rowset`,
//       payload,
//       {
//         headers: {
//           Authorization: `Bearer ${token}`,
//           "Content-Type": "application/json"
//         }
//       }
//     );

//     log(
//       "INFO",
//       `Saved to DE | Phone: ${from} | Name: ${profileName} | AmountType: ${amountType} | PartialAmount: ${partialAmount} | FinalAmount: ${finalAmount} | Tenure: ${tenure} | MsgID: ${messageId} | Time: ${timestamp}`
//     );
//   } catch (err) {
//     if (err.response?.status === 401) {
//       tokenCache = { value: null, expiresAt: null };
//     }
//     log("ERROR", "Failed to save Flow data to DE:", {
//       status: err.response?.status,
//       data: err.response?.data,
//       message: err.message
//     });
//   }
// }

// // ─── Diagnostic endpoint ──────────────────────────────────────────────────────
// // Hit GET /test-fetch/:phone to test DE lookup independently of the webhook
// // e.g. curl http://localhost:3000/test-fetch/919149074149
// app.get("/test-fetch/:phone", async (req, res) => {
//   const phone = req.params.phone;
//   log("INFO", `[TEST] Manual fetch triggered for phone: ${phone}`);

//   try {
//     const token = await getSFMCToken();

//     // Try all three plausible endpoint variants and return all results
//     const results = {};

//     // Variant 1: OData filter on Subscriber_Key
//     try {
//       const url1 = `https://${SFMC.subdomain}.rest.marketingcloudapis.com/data/v1/customobjectdata/key/${SFMC.fdAmountDeKey}/rowset?$filter=Subscriber_Key%20eq%20'${phone}'`;
//       const r1 = await axios.get(url1, { headers: { Authorization: `Bearer ${token}` } });
//       results.variant1_odata_subscriber_key = { status: r1.status, data: r1.data };
//     } catch (e) {
//       results.variant1_odata_subscriber_key = { status: e.response?.status, error: e.response?.data || e.message };
//     }

//     // Variant 2: OData filter on Mobile
//     try {
//       const url2 = `https://${SFMC.subdomain}.rest.marketingcloudapis.com/data/v1/customobjectdata/key/${SFMC.fdAmountDeKey}/rowset?$filter=Mobile%20eq%20'${phone}'`;
//       const r2 = await axios.get(url2, { headers: { Authorization: `Bearer ${token}` } });
//       results.variant2_odata_mobile = { status: r2.status, data: r2.data };
//     } catch (e) {
//       results.variant2_odata_mobile = { status: e.response?.status, error: e.response?.data || e.message };
//     }

//     // Variant 3: Fetch ALL rows (no filter) — confirms endpoint works + shows real field names
//     try {
//       const url3 = `https://${SFMC.subdomain}.rest.marketingcloudapis.com/data/v1/customobjectdata/key/${SFMC.fdAmountDeKey}/rowset`;
//       const r3 = await axios.get(url3, { headers: { Authorization: `Bearer ${token}` } });
//       results.variant3_all_rows = { status: r3.status, data: r3.data };
//     } catch (e) {
//       results.variant3_all_rows = { status: e.response?.status, error: e.response?.data || e.message };
//     }

//     log("INFO", "[TEST] All variant results:", results);
//     return res.status(200).json(results);
//   } catch (err) {
//     return res.status(500).json({ error: err.message });
//   }
// });
// // ─────────────────────────────────────────────────────────────────────────────

// app.get("/webhook", (req, res) => {
//   const mode = req.query["hub.mode"];
//   const token = req.query["hub.verify_token"];
//   const challenge = req.query["hub.challenge"];

//   if (mode === "subscribe" && token === VERIFY_TOKEN) {
//     log("INFO", "Webhook verified");
//     return res.status(200).send(challenge);
//   }

//   return res.sendStatus(403);
// });

// app.post("/webhook", async (req, res) => {
//   const body = req.body;

//   if (body.object !== "whatsapp_business_account") {
//     return res
//       .status(404)
//       .json({ status: "error", message: "Not a whatsapp_business_account event" });
//   }

//   const collectedFlowData = [];

//   try {
//     const entries = body.entry || [];

//     for (const entry of entries) {
//       const changes = entry.changes || [];

//       for (const change of changes) {
//         const messages = change.value?.messages || [];
//         const contacts = change.value?.contacts || [];

//         for (const message of messages) {
//           const from = message.from;
//           const messageId = message.id;
//           const timestamp = new Date(message.timestamp * 1000).toISOString();

//           const contact = contacts.find((c) => c.wa_id === from);
//           const profileName = contact?.profile?.name || "";

//           if (
//             message.type === "interactive" &&
//             message.interactive?.type === "nfm_reply"
//           ) {
//             const flowResponse = message.interactive.nfm_reply?.response_json;

//             if (!flowResponse) {
//               log("DEBUG", "No response_json in flow reply, skipping");
//               continue;
//             }

//             let flowData;
//             try {
//               flowData =
//                 typeof flowResponse === "string"
//                   ? JSON.parse(flowResponse)
//                   : flowResponse;
//             } catch (parseErr) {
//               log("ERROR", "Failed to parse flow response_json");
//               continue;
//             }

//             const amountType = flowData?.amount_type ?? null;
//             const partialAmount = flowData?.partial_amount ?? null;
//             const tenure = flowData?.tenure ?? null;

//             if (amountType === null && tenure === null) {
//               log("DEBUG", "Flow response missing amount_type and tenure, skipping");
//               continue;
//             }

//             let finalAmount = null;

//             if (amountType === "full") {
//               log("INFO", `amount_type is "full" — fetching FD_Amount from Internal_bhav DE for Subscriber_Key: ${from}`);
//               finalAmount = await fetchFDAmountFromDE(from);
//             } else {
//               finalAmount = partialAmount;
//             }

//             log(
//               "INFO",
//               `Flow Data Received → Phone: ${from} | Name: ${profileName} | AmountType: ${amountType} | PartialAmount: ${partialAmount} | FinalAmount: ${finalAmount} | Tenure: ${tenure} | MsgID: ${messageId} | Time: ${timestamp}`
//             );

//             collectedFlowData.push({
//               phone: from,
//               profileName,
//               amountType,
//               partialAmount,
//               finalAmount,
//               tenure,
//               messageId,
//               timestamp
//             });

//             saveFlowDataToDE({
//               from,
//               profileName,
//               amountType,
//               partialAmount,
//               finalAmount,
//               tenure,
//               messageId,
//               timestamp
//             });
//           }
//         }
//       }
//     }
//   } catch (err) {
//     log("ERROR", "Webhook processing failed:", err.message);
//     return res.status(500).json({ status: "error", message: err.message });
//   }

//   return res.status(200).json({
//     status: "ok",
//     received: collectedFlowData.length,
//     flowData: collectedFlowData
//   });
// });

// const PORT = process.env.PORT || 3000;
// app.listen(PORT, () => {
//   log("INFO", `Server running on port ${PORT}`);
// });
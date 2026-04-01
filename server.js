const express = require("express");
const cors = require("cors");
const { BedrockRuntimeClient, InvokeModelCommand } = require("@aws-sdk/client-bedrock-runtime");

const app = express();
const PORT = process.env.PORT || 3001;

app.use(cors({ origin: "*" }));
app.use(express.json({ limit: "10mb" }));

// ─── AWS Bedrock Client ───────────────────────────────────────────────────────
const client = new BedrockRuntimeClient({
  region: process.env.AWS_REGION || "us-east-1",
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  },
});

const BEDROCK_MODEL_ID = "us.anthropic.claude-sonnet-4-5-20250929-v1:0";

// ─── Supabase Client ──────────────────────────────────────────────────────────
const { createClient } = require("@supabase/supabase-js");
const pdfParse = require("pdf-parse");
const { parseCMS2567 } = require("./cms2567Parser");
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SECRET_KEY  // service role — bypasses RLS for server writes
);

// ─── In-memory batch job store ────────────────────────────────────────────────
// Render Pro keeps the process alive — safe to store in memory between requests
const batchJobs = new Map();

// ─── POC Generation Helper ────────────────────────────────────────────────────
// Default signers — always saved so the signature panel renders correctly
const DEFAULT_SIGNERS = [
  { id: "administrator", role: "Administrator", credential: "NHA", name: "", date: "", signatureData: null, signedAt: null, status: "pending", method: null, required: true },
  { id: "don", role: "Director of Nursing", credential: "RN, DON", name: "", date: "", signatureData: null, signedAt: null, status: "pending", method: null, required: false },
  { id: "medical_director", role: "Medical Director", credential: "MD", name: "", date: "", signatureData: null, signedAt: null, status: "pending", method: null, required: false },
];

async function generatePOCOnServer(citation, facility, guidance, includeDates) {
  const systemPrompt = `You are a healthcare compliance specialist. Generate a complete Plan of Correction for a CMS deficiency citation. Return ONLY valid JSON, no markdown. Format: {"statement_of_deficiency":"","root_cause_analysis":"","immediate_corrective_actions":"","residents_affected":"","systemic_changes":"","education_and_training":"","policy_procedure_review":"","monitoring_and_auditing":"","sustainability_plan":"","projected_compliance_date":"","attestation":""}`;

  const dateInstruction = includeDates
    ? "Include specific calendar dates in corrective actions."
    : "Use relative timeframes only (e.g. 'within 30 days', 'immediately', 'ongoing monthly'). Do NOT include specific calendar dates in the narrative. The projected_compliance_date field should still be a valid YYYY-MM-DD date.";

  // raw_block / full_deficiency_text is the verbatim surveyor text — primary context for POC
  // It is never displayed to the user but used here to generate a specific, accurate POC
  // Send the full verbatim block — no truncation. Even the longest CMS-2567 citation
  // is ~20,000 chars which is well within Claude's context window.
  const deficiencyContext = citation.full_deficiency_text || citation.raw_block || citation.deficiency_narrative_full || citation.deficiency_statement || citation.deficiency_summary || "";

  const userPrompt = `FACILITY: ${facility.facility_name} (${facility.facility_type || "SNF"}) | CCN: ${facility.facility_id || ""} | State: ${facility.state || ""}
TAG: ${citation.tags?.join(", ")} | Survey Date: ${citation.survey_date} | Scope/Severity: ${citation.scope_severity}
${citation.title ? "REGULATION: " + citation.title : ""}
${citation.cfr_citation ? "CFR: " + citation.cfr_citation : ""}
${citation.initial_comments ? "SURVEY CONTEXT (Initial Comments): " + citation.initial_comments : ""}

VERBATIM DEFICIENCY TEXT FROM CMS-2567 (do not quote or repeat this in the POC — respond to it):
${deficiencyContext}

${citation.harm_or_risk_statement ? "HARM/RISK IDENTIFIED: " + citation.harm_or_risk_statement : ""}
${citation.residents_affected || citation.resident_impact ? "RESIDENTS AFFECTED: " + (citation.residents_affected || citation.resident_impact) : ""}
Compliance Date: ${citation.projected_compliance_date || "10 days from survey date"}
${guidance ? "GUIDANCE: " + guidance.slice(0, 1500) : ""}
${dateInstruction}
Generate a complete, CMS-acceptable Plan of Correction that specifically addresses the deficiency above. Return ONLY the JSON object.`;

  const bedrockBody = {
    anthropic_version: "bedrock-2023-05-31",
    max_tokens: 16000,
    system: systemPrompt,
    messages: [{ role: "user", content: userPrompt }],
  };

  const command = new InvokeModelCommand({
    modelId: BEDROCK_MODEL_ID,
    contentType: "application/json",
    accept: "application/json",
    body: JSON.stringify(bedrockBody),
  });

  const response = await client.send(command);
  const data = JSON.parse(new TextDecoder().decode(response.body));
  const text = data?.content?.[0]?.text || "";
  const start = text.indexOf("{");
  const end = text.lastIndexOf("}");
  if (start === -1 || end === -1) throw new Error("No JSON in response");
  return JSON.parse(text.slice(start, end + 1));
}

// ─── Background Batch Runner ──────────────────────────────────────────────────
async function runBatchJob(batchId, citations, facility, settings, userId, facilityId) {
  const job = batchJobs.get(batchId);
  if (!job) return;

  const batchLabel = "Batch " + new Date().toLocaleDateString() + " — " + citations.length + " citation" + (citations.length !== 1 ? "s" : "");

  const CONCURRENCY = 4;
  const queue = [...citations.map((c, i) => ({ ...c, _idx: i }))];
  const results = new Array(citations.length).fill(null);

  const processOne = async (item) => {
    const { _idx, ...citation } = item;
    let pipData = null;
    let lastError = null;

    // F0000 = Initial Comments — no POC generated, stored as display-only record
    const isF0000 = (citation.tags || []).some(t => t === 'F0000' || t === 'F000');
    if (isF0000) {
      pipData = {
        statement_of_deficiency: citation.full_deficiency_text || citation.deficiency_statement || "",
        root_cause_analysis: "",
        immediate_corrective_actions: "",
        residents_affected: "",
        systemic_changes: "",
        education_and_training: "",
        policy_procedure_review: "",
        monitoring_and_auditing: "",
        sustainability_plan: "",
        projected_compliance_date: "",
        attestation: "",
        _initial_comments: true,
      };
    } else {
      for (let attempt = 0; attempt < 2; attempt++) {
        try {
          const guidance = "";
          pipData = await generatePOCOnServer(citation, facility, guidance, settings.includeDates !== false);
          break;
        } catch (err) {
          lastError = err;
          if (attempt === 0) await new Promise(r => setTimeout(r, 1500));
        }
      }
    }

    if (pipData) {
      const pocId = require("crypto").randomUUID();
      const newPip = {
        id: pocId,
        facility: facility.facility_name,
        facility_id: facilityId,
        tags: citation.tags || [],
        survey_date: citation.survey_date,
        survey_type: citation.survey_type,
        scope_severity: citation.scope_severity,
        created_date: new Date().toISOString().split("T")[0],
        mode: "AI",
        mode_reason: "Claude API — Server Batch",
        status: "Draft",
        sections: pipData,
        citation_data: {
          ...citation,
          // survey_metadata: use what frontend sent, fall back to what the parse job extracted
          // This ensures header data always survives to export time
          survey_metadata: (citation.survey_metadata && Object.keys(citation.survey_metadata).length > 0)
            ? citation.survey_metadata
            : (job?.result?.survey_metadata || {}),
          // Ensure verbatim fields always present
          full_deficiency_text: citation.full_deficiency_text || citation.raw_block || citation.deficiency_narrative_full || citation.deficiency_statement || "",
          // Persist initial_comments onto every citation so it survives navigation/session
          initial_comments: citation.initial_comments || job?.result?.initial_comments || "",
        },
        batch_id: batchId,
        source_document: settings.sourceDocument || null,
        version_history: [],
        export_history: [],
      };

      // Save to Supabase immediately — all fields needed by dbToLocal mapping
      try {
        await supabase.from("pocs").insert({
          id: newPip.id,
          facility_id: facilityId,
          facility_name: facility.facility_name,
          tags: newPip.tags,
          survey_date: newPip.survey_date,
          survey_type: newPip.survey_type,
          scope_severity: newPip.scope_severity,
          status: "Draft",
          mode: "AI",
          mode_reason: "Claude API — Server Batch",
          batch_id: batchId,
          batch_label: batchLabel,
          sections: newPip.sections,
          citation_data: newPip.citation_data,
          signers: DEFAULT_SIGNERS,
          guidance_used: [],
          export_history: [],
          version_history: [],
          archive_outcome: "pending",
          archived_off: false,
          source_document: settings.sourceDocument || null,
          created_at: new Date().toISOString(),
        });
        console.log(`[Batch ${batchId}] Saved POC for ${citation.tags?.join(",")}`);
      } catch (dbErr) {
        console.error(`[Batch ${batchId}] Supabase save FAILED for ${citation.tags?.join(",")}: ${dbErr.message}`);
        // Still count as done — POC was generated even if save failed
        // Don't silently swallow — log full error
        console.error(dbErr);
      }

      results[_idx] = newPip;
      job.done++;
    } else {
      console.warn(`[Batch ${batchId}] pipData null for ${citation.tags?.join(",") || "unknown"} — lastError: ${lastError?.message}`);
      job.failed++;
      job.errors.push(`${citation.tags?.join(",")}: ${lastError?.message || "Unknown error"}`);
    }

    job.current = job.done + job.failed;
    job.completedPips = results.filter(Boolean);
  };

  // Run with concurrency limit
  const workers = Array(Math.min(CONCURRENCY, queue.length)).fill(null).map(async () => {
    while (queue.length > 0) {
      const item = queue.shift();
      if (item) await processOne(item);
    }
  });

  await Promise.all(workers);
  job.status = "complete";
  job.completedAt = new Date().toISOString();
  console.log(`[Batch ${batchId}] Complete — ${job.done} done, ${job.failed} failed`);
}

// ─── SendGrid Email Helper ────────────────────────────────────────────────────
async function sendEmail(to, subject, htmlBody, textBody) {
  const apiKey = process.env.SENDGRID_API_KEY;
  const fromEmail = process.env.FROM_EMAIL || "notifications@survaihealth.com";

  if (!apiKey) {
    console.warn("SENDGRID_API_KEY not set — skipping email to", to);
    return { success: false, reason: "No API key" };
  }

  const response = await fetch("https://api.sendgrid.com/v3/mail/send", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${apiKey}`,
    },
    body: JSON.stringify({
      personalizations: [{ to: [{ email: to }] }],
      from: { email: fromEmail, name: "SurvAIHealth" },
      subject,
      content: [
        { type: "text/plain", value: textBody || subject },
        { type: "text/html", value: htmlBody },
      ],
    }),
  });

  if (!response.ok) {
    const err = await response.text();
    console.error("SendGrid error:", err);
    return { success: false, reason: err };
  }

  return { success: true };
}

// ─── Email Templates ──────────────────────────────────────────────────────────
function mfaEmailHtml(name, code) {
  return `<!DOCTYPE html><html><head><meta charset="utf-8"></head>
<body style="margin:0;padding:0;background:#f1f5f9;font-family:Arial,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#f1f5f9;padding:40px 20px;">
    <tr><td align="center">
      <table width="600" cellpadding="0" cellspacing="0" style="background:white;border-radius:12px;overflow:hidden;box-shadow:0 4px 24px rgba(0,0,0,0.08);">
        <tr><td style="background:linear-gradient(135deg,#0b3660,#0f4c81);padding:28px 40px;text-align:center;">
          <div style="font-size:26px;font-weight:800;color:white;">SurvAI<span style="color:#38bdf8;">Health</span></div>
          <div style="color:#93c5fd;font-size:12px;margin-top:4px;text-transform:uppercase;letter-spacing:0.1em;">Verification Code</div>
        </td></tr>
        <tr><td style="padding:40px;">
          <h2 style="color:#0f172a;font-size:20px;margin:0 0 16px;">Your Sign-In Verification Code</h2>
          <p style="color:#475569;font-size:15px;line-height:1.6;margin:0 0 28px;">Hello ${name || ""},<br><br>Use the code below to complete your sign-in to SurvAIHealth.</p>
          <div style="text-align:center;margin:28px 0;">
            <div style="display:inline-block;background:#f0f9ff;border:2px solid #bae6fd;border-radius:12px;padding:24px 48px;">
              <div style="font-size:40px;font-weight:800;letter-spacing:0.4em;color:#0f4c81;font-family:'Courier New',monospace;">${code}</div>
            </div>
          </div>
          <div style="background:#fef9c3;border:1px solid #fde047;border-radius:8px;padding:14px 18px;margin-bottom:24px;">
            <div style="color:#854d0e;font-size:13px;">This code expires in <strong>10 minutes</strong>. Do not share it with anyone.</div>
          </div>
          <p style="color:#94a3b8;font-size:13px;line-height:1.6;margin:0;">If you did not attempt to sign in, please contact your administrator immediately.</p>
        </td></tr>
        <tr><td style="background:#f8fafc;border-top:1px solid #e2e8f0;padding:16px 40px;text-align:center;">
          <p style="color:#94a3b8;font-size:12px;margin:0;">SurvAIHealth LLC &middot; Quality Care, Intelligently Managed</p>
        </td></tr>
      </table>
    </td></tr>
  </table>
</body></html>`;
}

function resetEmailHtml(name, code) {
  return `<!DOCTYPE html><html><head><meta charset="utf-8"></head>
<body style="margin:0;padding:0;background:#f1f5f9;font-family:Arial,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#f1f5f9;padding:40px 20px;">
    <tr><td align="center">
      <table width="600" cellpadding="0" cellspacing="0" style="background:white;border-radius:12px;overflow:hidden;box-shadow:0 4px 24px rgba(0,0,0,0.08);">
        <tr><td style="background:linear-gradient(135deg,#0b3660,#0f4c81);padding:28px 40px;text-align:center;">
          <div style="font-size:26px;font-weight:800;color:white;">SurvAI<span style="color:#38bdf8;">Health</span></div>
          <div style="color:#93c5fd;font-size:12px;margin-top:4px;text-transform:uppercase;letter-spacing:0.1em;">Password Reset</div>
        </td></tr>
        <tr><td style="padding:40px;">
          <h2 style="color:#0f172a;font-size:20px;margin:0 0 16px;">Password Reset Request</h2>
          <p style="color:#475569;font-size:15px;line-height:1.6;margin:0 0 28px;">Hello ${name || ""},<br><br>We received a request to reset your SurvAIHealth password. Enter the code below to continue.</p>
          <div style="text-align:center;margin:28px 0;">
            <div style="display:inline-block;background:#fffbeb;border:2px solid #fde68a;border-radius:12px;padding:24px 48px;">
              <div style="font-size:40px;font-weight:800;letter-spacing:0.4em;color:#92400e;font-family:'Courier New',monospace;">${code}</div>
            </div>
          </div>
          <div style="background:#fef9c3;border:1px solid #fde047;border-radius:8px;padding:14px 18px;margin-bottom:24px;">
            <div style="color:#854d0e;font-size:13px;">This code expires in <strong>15 minutes</strong>.</div>
          </div>
          <p style="color:#94a3b8;font-size:13px;line-height:1.6;margin:0;">If you did not request a password reset, you can safely ignore this email. Your password will not change.</p>
        </td></tr>
        <tr><td style="background:#f8fafc;border-top:1px solid #e2e8f0;padding:16px 40px;text-align:center;">
          <p style="color:#94a3b8;font-size:12px;margin:0;">SurvAIHealth LLC &middot; Quality Care, Intelligently Managed</p>
        </td></tr>
      </table>
    </td></tr>
  </table>
</body></html>`;
}

function welcomeEmailHtml(name, email, tempPassword, role, facilityName, isReactivation = false) {
  return `<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"></head>
<body style="margin:0;padding:0;background:#f1f5f9;font-family:Arial,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#f1f5f9;padding:40px 20px;">
    <tr><td align="center">
      <table width="600" cellpadding="0" cellspacing="0" style="background:white;border-radius:12px;overflow:hidden;box-shadow:0 4px 24px rgba(0,0,0,0.08);">
        <tr><td style="background:linear-gradient(135deg,#0b3660,#0f4c81);padding:32px 40px;text-align:center;">
          <div style="font-size:28px;font-weight:800;color:white;">SurvAI<span style="color:#38bdf8;">Health</span></div>
          <div style="color:#93c5fd;font-size:13px;margin-top:6px;text-transform:uppercase;letter-spacing:0.1em;">Quality Care, Intelligently Managed</div>
        </td></tr>
        <tr><td style="padding:40px;">
          <h2 style="color:#0f172a;font-size:22px;margin:0 0 8px;">${isReactivation ? "Your Account Has Been Reactivated" : "Welcome to SurvAIHealth, " + name + "!"}</h2>
          <p style="color:#475569;font-size:15px;line-height:1.6;margin:0 0 24px;">${isReactivation ? "Your account has been reactivated. Use the credentials below to sign back in." : "Your account has been created. You can now log in and start managing Plans of Correction."}</p>
          <table width="100%" cellpadding="0" cellspacing="0" style="background:#f0f9ff;border:1px solid #bae6fd;border-radius:10px;margin-bottom:28px;">
            <tr><td style="padding:24px;">
              <div style="font-size:13px;font-weight:700;color:#0369a1;text-transform:uppercase;letter-spacing:0.08em;margin-bottom:16px;">Your Login Credentials</div>
              <table width="100%">
                <tr><td style="padding:8px 0;color:#64748b;font-size:14px;width:130px;">Email:</td><td style="padding:8px 0;color:#0f172a;font-size:14px;font-weight:600;">${email}</td></tr>
                <tr><td style="padding:8px 0;color:#64748b;font-size:14px;">Temp Password:</td><td style="padding:8px 0;"><span style="background:#0f4c81;color:white;padding:6px 14px;border-radius:6px;font-size:15px;font-weight:700;font-family:monospace;">${tempPassword}</span></td></tr>
                <tr><td style="padding:8px 0;color:#64748b;font-size:14px;">Role:</td><td style="padding:8px 0;color:#0f172a;font-size:14px;font-weight:600;">${role}</td></tr>
                ${facilityName ? `<tr><td style="padding:8px 0;color:#64748b;font-size:14px;">Facility:</td><td style="padding:8px 0;color:#0f172a;font-size:14px;font-weight:600;">${facilityName}</td></tr>` : ""}
              </table>
            </td></tr>
          </table>
          <div style="background:#fff7ed;border:1px solid #fed7aa;border-radius:8px;padding:16px 20px;margin-bottom:28px;">
            <div style="color:#92400e;font-size:14px;"><strong>Important:</strong> Please change your password after your first login.</div>
          </div>
          <table width="100%"><tr><td align="center" style="padding:8px 0 28px;">
            <a href="https://survaihealth.com" style="background:linear-gradient(135deg,#0f4c81,#0891b2);color:white;text-decoration:none;padding:14px 36px;border-radius:8px;font-size:15px;font-weight:700;display:inline-block;">Sign In to SurvAIHealth</a>
          </td></tr></table>
        </td></tr>
        <tr><td style="background:#f8fafc;border-top:1px solid #e2e8f0;padding:20px 40px;text-align:center;">
          <p style="color:#94a3b8;font-size:12px;margin:0;">SurvAIHealth LLC &middot; Quality Care, Intelligently Managed</p>
        </td></tr>
      </table>
    </td></tr>
  </table>
</body></html>`;
}

function deadlineReminderHtml(facilityName, tags, surveyDate, complianceDate, daysLeft) {
  const urgencyColor = daysLeft < 0 ? "#dc2626" : daysLeft <= 3 ? "#ea580c" : daysLeft <= 7 ? "#d97706" : "#0891b2";
  const urgencyBg = daysLeft < 0 ? "#fef2f2" : daysLeft <= 3 ? "#fff7ed" : daysLeft <= 7 ? "#fffbeb" : "#e0f2fe";
  const urgencyBorder = daysLeft < 0 ? "#fecaca" : daysLeft <= 3 ? "#fed7aa" : daysLeft <= 7 ? "#fde68a" : "#bae6fd";
  const urgencyLabel = daysLeft < 0 ? `OVERDUE BY ${Math.abs(daysLeft)} DAY${Math.abs(daysLeft) !== 1 ? "S" : ""}` : daysLeft === 0 ? "DUE TODAY" : `${daysLeft} DAY${daysLeft !== 1 ? "S" : ""} REMAINING`;
  const urgencyEmoji = daysLeft < 0 ? "🚨" : daysLeft <= 3 ? "🔴" : daysLeft <= 7 ? "🟡" : "📅";
  return `<!DOCTYPE html><html><head><meta charset="utf-8"></head>
<body style="margin:0;padding:0;background:#f1f5f9;font-family:Arial,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#f1f5f9;padding:40px 20px;">
    <tr><td align="center">
      <table width="600" cellpadding="0" cellspacing="0" style="background:white;border-radius:12px;overflow:hidden;">
        <tr><td style="background:linear-gradient(135deg,#0b3660,#0f4c81);padding:28px 40px;text-align:center;">
          <div style="font-size:26px;font-weight:800;color:white;">SurvAI<span style="color:#38bdf8;">Health</span></div>
          <div style="color:#93c5fd;font-size:12px;margin-top:4px;text-transform:uppercase;letter-spacing:0.1em;">Compliance Deadline Alert</div>
        </td></tr>
        <tr><td style="background:${urgencyBg};border-bottom:2px solid ${urgencyBorder};padding:20px 40px;text-align:center;">
          <div style="font-size:32px;margin-bottom:8px;">${urgencyEmoji}</div>
          <div style="font-size:22px;font-weight:800;color:${urgencyColor};">${urgencyLabel}</div>
        </td></tr>
        <tr><td style="padding:32px 40px;">
          <table width="100%" cellpadding="0" cellspacing="0" style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:10px;margin-bottom:24px;">
            <tr><td style="padding:20px 24px;">
              <tr><td style="padding:6px 0;color:#64748b;font-size:14px;width:160px;">Facility:</td><td style="padding:6px 0;color:#0f172a;font-size:14px;font-weight:700;">${facilityName}</td></tr>
              <tr><td style="padding:6px 0;color:#64748b;font-size:14px;">Tags:</td><td style="padding:6px 0;color:#0f172a;font-size:14px;">${tags.join(", ")}</td></tr>
              <tr><td style="padding:6px 0;color:#64748b;font-size:14px;">Survey Date:</td><td style="padding:6px 0;color:#0f172a;font-size:14px;">${surveyDate}</td></tr>
              <tr><td style="padding:6px 0;color:#64748b;font-size:14px;">Compliance Date:</td><td style="padding:6px 0;color:${urgencyColor};font-size:15px;font-weight:800;">${complianceDate}</td></tr>
            </td></tr>
          </table>
          <table width="100%"><tr><td align="center" style="padding:4px 0 24px;">
            <a href="https://survaihealth.com" style="background:linear-gradient(135deg,#0f4c81,#0891b2);color:white;text-decoration:none;padding:13px 32px;border-radius:8px;font-size:14px;font-weight:700;display:inline-block;">View Plan of Correction</a>
          </td></tr></table>
        </td></tr>
        <tr><td style="background:#f8fafc;border-top:1px solid #e2e8f0;padding:16px 40px;text-align:center;">
          <p style="color:#94a3b8;font-size:12px;margin:0;">SurvAIHealth LLC &middot; Quality Care, Intelligently Managed</p>
        </td></tr>
      </table>
    </td></tr>
  </table>
</body></html>`;
}

// ─── Health Check ─────────────────────────────────────────────────────────────
app.get("/", (req, res) => res.json({ status: "SurvAI API running on Bedrock" }));

// ─── Main Claude Proxy ────────────────────────────────────────────────────────
app.post("/api/claude", async (req, res) => {
  const body = { ...req.body };
  delete body.apiKey;
  const { model, ...rest } = body;
  const bedrockBody = {
    anthropic_version: "bedrock-2023-05-31",
    max_tokens: rest.max_tokens || 6000,
    messages: rest.messages || [],
    ...(rest.system ? { system: rest.system } : {}),
    ...(rest.temperature !== undefined ? { temperature: rest.temperature } : {}),
  };
  try {
    const command = new InvokeModelCommand({
      modelId: BEDROCK_MODEL_ID,
      contentType: "application/json",
      accept: "application/json",
      body: JSON.stringify(bedrockBody),
    });
    const response = await client.send(command);
    const data = JSON.parse(new TextDecoder().decode(response.body));
    return res.status(200).json(data);
  } catch (err) {
    console.error("Bedrock error:", err.message);
    return res.status(500).json({ error: { message: err.message } });
  }
});

// ─── MFA Verification Code ────────────────────────────────────────────────────
app.post("/api/email/mfa", async (req, res) => {
  const { email, name, code } = req.body;
  if (!email || !code) return res.status(400).json({ error: "email and code are required" });
  const html = mfaEmailHtml(name, code);
  const text = `Your SurvAIHealth verification code is: ${code}\n\nThis code expires in 10 minutes. Do not share it with anyone.`;
  const result = await sendEmail(email, `${code} is your SurvAIHealth verification code`, html, text);
  if (!result.success) return res.status(500).json({ error: "Failed to send MFA email", detail: result.reason });
  console.log(`MFA code sent to ${email}`);
  return res.status(200).json({ success: true });
});

// ─── Password Reset Code ──────────────────────────────────────────────────────
app.post("/api/email/reset", async (req, res) => {
  const { email, name, code } = req.body;
  if (!email || !code) return res.status(400).json({ error: "email and code are required" });
  const html = resetEmailHtml(name, code);
  const text = `Your SurvAIHealth password reset code is: ${code}\n\nThis code expires in 15 minutes.\n\nIf you did not request a password reset, ignore this email.`;
  const result = await sendEmail(email, "SurvAIHealth Password Reset Code", html, text);
  if (!result.success) return res.status(500).json({ error: "Failed to send reset email", detail: result.reason });
  console.log(`Password reset code sent to ${email}`);
  return res.status(200).json({ success: true });
});

// ─── Welcome Email ────────────────────────────────────────────────────────────
app.post("/api/email/welcome", async (req, res) => {
  const { name, email, tempPassword, role, facilityName } = req.body;
  if (!name || !email || !tempPassword) return res.status(400).json({ error: "name, email, and tempPassword are required" });
  const roleLabels = { admin: "System Administrator", regional: "Regional Director", facility_admin: "Facility Administrator", editor: "Editor", staff: "Staff Member", viewer: "Viewer" };
  const isReactivation = req.body.isReactivation === true;
  const subject = isReactivation ? "Your SurvAIHealth Account Has Been Reactivated" : "Welcome to SurvAIHealth — Your Account is Ready";
  const html = welcomeEmailHtml(name, email, tempPassword, roleLabels[role] || role, facilityName, isReactivation);
  const text = `Welcome to SurvAIHealth, ${name}!\n\nEmail: ${email}\nTemporary Password: ${tempPassword}\nRole: ${roleLabels[role] || role}${facilityName ? `\nFacility: ${facilityName}` : ""}\n\nSign in at: https://survaihealth.com\n\nSurvAIHealth LLC`;
  const result = await sendEmail(email, subject, html, text);
  if (!result.success) return res.status(500).json({ error: "Failed to send email", detail: result.reason });
  console.log(`Welcome email sent to ${email}`);
  return res.status(200).json({ success: true, message: `Welcome email sent to ${email}` });
});

// ─── Deadline Reminder Email ──────────────────────────────────────────────────
app.post("/api/email/deadline", async (req, res) => {
  const { recipients, facilityName, tags, surveyDate, complianceDate, daysLeft } = req.body;
  if (!recipients || !recipients.length || !facilityName || !complianceDate) return res.status(400).json({ error: "recipients, facilityName, and complianceDate are required" });
  const urgencyLabel = daysLeft < 0 ? `OVERDUE — ${facilityName}` : daysLeft === 0 ? `Due Today — ${facilityName}` : `${daysLeft} Days Remaining — ${facilityName}`;
  const subject = `${urgencyLabel} POC Deadline`;
  const html = deadlineReminderHtml(facilityName, tags || [], surveyDate, complianceDate, daysLeft);
  const text = `SurvAIHealth Deadline Alert\n\nFacility: ${facilityName}\nTags: ${(tags || []).join(", ")}\nCompliance Date: ${complianceDate}\n\nLog in at https://survaihealth.com\n\nSurvAIHealth LLC`;
  const results = await Promise.all(recipients.map(email => sendEmail(email, subject, html, text)));
  const failed = results.filter(r => !r.success);
  console.log(`Deadline reminder sent to ${recipients.length - failed.length}/${recipients.length} recipients`);
  return res.status(200).json({ success: true, sent: recipients.length - failed.length, failed: failed.length });
});

// ─── Demo Request Email ───────────────────────────────────────────────────────
app.post("/api/email/demo", async (req, res) => {
  const { name, email, facility, role, facilities, message } = req.body;
  if (!name || !email) return res.status(400).json({ error: "Name and email are required" });
  const subject = `New Demo Request — ${facility || "Unknown Facility"} (${role || "Unknown Role"})`;
  const html = `<!DOCTYPE html><html><head><meta charset="utf-8"></head><body style="margin:0;padding:0;background:#f1f5f9;font-family:Arial,sans-serif;"><table width="100%" cellpadding="0" cellspacing="0" style="background:#f1f5f9;padding:40px 20px;"><tr><td align="center"><table width="600" cellpadding="0" cellspacing="0" style="background:white;border-radius:12px;overflow:hidden;"><tr><td style="background:linear-gradient(135deg,#0b3660,#0f4c81);padding:28px 40px;"><div style="font-size:22px;font-weight:800;color:white;">SurvAI<span style="color:#38bdf8;">Health</span></div><div style="color:#93c5fd;font-size:12px;margin-top:4px;">New Demo Request</div></td></tr><tr><td style="padding:32px 40px;"><h2 style="color:#0f172a;font-size:20px;margin:0 0 24px;">New Demo Request</h2><table width="100%" style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:10px;"><tr><td style="padding:20px 24px;"><table width="100%"><tr><td style="padding:8px 0;color:#64748b;font-size:14px;width:140px;">Name:</td><td style="padding:8px 0;color:#0f172a;font-size:14px;font-weight:700;">${name}</td></tr><tr><td style="padding:8px 0;color:#64748b;font-size:14px;">Email:</td><td style="padding:8px 0;color:#0891b2;font-size:14px;">${email}</td></tr><tr><td style="padding:8px 0;color:#64748b;font-size:14px;">Facility:</td><td style="padding:8px 0;color:#0f172a;font-size:14px;">${facility || "Not provided"}</td></tr><tr><td style="padding:8px 0;color:#64748b;font-size:14px;">Role:</td><td style="padding:8px 0;color:#0f172a;font-size:14px;">${role || "Not provided"}</td></tr><tr><td style="padding:8px 0;color:#64748b;font-size:14px;">Facilities:</td><td style="padding:8px 0;color:#0f172a;font-size:14px;">${facilities || "1 facility"}</td></tr>${message ? `<tr><td style="padding:8px 0;color:#64748b;font-size:14px;vertical-align:top;">Message:</td><td style="padding:8px 0;color:#0f172a;font-size:14px;">${message}</td></tr>` : ""}</table></td></tr></table></td></tr><tr><td style="background:#f8fafc;border-top:1px solid #e2e8f0;padding:16px 40px;text-align:center;"><p style="color:#94a3b8;font-size:12px;margin:0;">SurvAIHealth LLC</p></td></tr></table></td></tr></table></body></html>`;
  const text = `New Demo Request\n\nName: ${name}\nEmail: ${email}\nFacility: ${facility || "Not provided"}\nRole: ${role || "Not provided"}\nMessage: ${message || "None"}\n\nReply to: ${email}`;
  const apiKey = process.env.SENDGRID_API_KEY;
  const fromEmail = process.env.FROM_EMAIL || "notifications@survaihealth.com";
  if (!apiKey) return res.status(500).json({ error: "Email not configured" });
  const response = await fetch("https://api.sendgrid.com/v3/mail/send", {
    method: "POST",
    headers: { "Content-Type": "application/json", Authorization: `Bearer ${apiKey}` },
    body: JSON.stringify({
      personalizations: [{ to: [{ email: fromEmail }] }],
      from: { email: fromEmail, name: "SurvAIHealth" },
      reply_to: { email: email, name: name },
      subject,
      content: [{ type: "text/plain", value: text }, { type: "text/html", value: html }],
    }),
  });
  if (!response.ok) { const err = await response.text(); console.error("SendGrid demo error:", err); return res.status(500).json({ error: "Failed to send email" }); }
  console.log(`Demo request from ${name} at ${facility}`);
  return res.status(200).json({ success: true });
});

// ─── CMS-2567 Citation Validator & Normalizer ────────────────────────────────

const BOILERPLATE_PATTERNS_VALIDATE = [
  /DEPARTMENT OF HEALTH AND HUMAN SERVICES/i,
  /CENTERS FOR MEDICARE (AND|&) MEDICAID SERVICES/i,
  /STATEMENT OF DEFICIENCIES AND PLAN OF CORRECTIONS?/i,
  /FORM APPROVED/i,
  /OMB\s*NO\./i,
  /PROVIDER\/SUPPLIER\/CLIA IDENTIFICATION NUMBER/i,
  /NAME OF PROVIDER OR SUPPLIER/i,
  /STREET ADDRESS,\s*CITY,\s*STATE,\s*ZIP/i,
  /SUMMARY STATEMENT OF DEFICIENCIES/i,
  /PROVIDER.?S PLAN OF CORRECTION/i,
  /FORM\s+CMS[-\s]?2567/i,
  /Previous Versions? Obsolete/i,
  /If continuation sheet Page/i,
  /Continued from page/i,
  /Event ID:/i,
  /Facility ID:/i,
];

function containsBoilerplate(text) {
  if (!text) return false;
  return BOILERPLATE_PATTERNS_VALIDATE.some(p => p.test(text));
}

function stripBoilerplateFromField(text) {
  if (!text) return text;
  let cleaned = text;
  for (const p of BOILERPLATE_PATTERNS_VALIDATE) {
    cleaned = cleaned.replace(new RegExp(p.source, "gi"), " ");
  }
  return cleaned.replace(/\s{3,}/g, " ").trim();
}

function validateAndScoreCitation(citation) {
  const hardErrors = [];
  const softErrors = [];
  const tag = (citation.tag_number || citation.tag || "").trim();

  // ── HARD fail rules — ONLY: missing tag OR completely empty block ─────────
  // Per spec: REMOVE hard fails for missing regulatory_title, CFR, scope_severity,
  // structured fields. Only hard fail if no tag or block is clearly empty.
  if (tag === "F0000") hardErrors.push("F0000 must not appear as a deficiency citation");
  if (!/^[FKE]\d{3,4}$/.test(tag)) hardErrors.push("Invalid tag_number: " + tag);

  // Only hard fail if both verbatim text AND narrative are completely empty
  const fullText = (citation.full_deficiency_text || "").trim();
  const narrative = (citation.deficiency_narrative_full || citation.deficiency_statement || citation.deficiency_summary || "").trim();
  if (!fullText && !narrative) hardErrors.push(tag + ": citation block is empty");
  else if (!fullText && narrative.length < 20) hardErrors.push(tag + ": citation block too short to be real");

  // Auto-strip boilerplate from narrative fields — soft flag, never hard fail
  const narrativeFields = ["deficiency_narrative_full","deficiency_summary","findings_full","observation_evidence","interview_evidence","record_review_evidence"];
  for (const field of narrativeFields) {
    if (containsBoilerplate(citation[field] || "")) {
      citation[field] = stripBoilerplateFromField(citation[field]);
      softErrors.push(tag + ": boilerplate auto-stripped from " + field);
    }
  }

  // ── SOFT flags — everything else goes to needs_review, never rejected ─────
  if (!citation.scope_severity?.trim()) softErrors.push(tag + ": missing scope_severity");
  if (!citation.regulatory_title?.trim()) softErrors.push(tag + ": missing regulatory_title");
  if (!citation.cfr_citations?.length) softErrors.push(tag + ": missing CFR citations");
  if ((citation.deficiency_summary || "").trim().length < 20) softErrors.push(tag + ": deficiency_summary short");
  const statePattern = /WAC\s+\d|RCW\s+\d|CMR\s+\d|\d{1,3}\s+ILCS/i;
  if (statePattern.test(narrative) && !(citation.state_regulatory_references?.length)) softErrors.push(tag + ": state reference not extracted");

  // Confidence scoring
  let noiseScore = 0;
  if (!citation.scope_severity) noiseScore += 0.2;
  if (!citation.cfr_citations?.length) noiseScore += 0.15;
  if (!citation.regulatory_title) noiseScore += 0.2;
  if (!citation.deficiency_summary) noiseScore += 0.15;
  if (hardErrors.length > 0) noiseScore = Math.min(1, noiseScore + 0.3);
  noiseScore = Math.round(Math.min(1, noiseScore) * 100) / 100;

  let boundaryConfidence = 1.0;
  if (/Continued from page/i.test(citation.deficiency_narrative_full || "")) boundaryConfidence -= 0.4;
  if (!citation.regulatory_title) boundaryConfidence -= 0.3;
  boundaryConfidence = Math.round(Math.max(0, boundaryConfidence) * 100) / 100;

  const parseConfidence = Math.round(Math.max(0, 1 - noiseScore - (softErrors.length * 0.05)) * 100) / 100;

  // Status
  let status = "approved_for_poc";
  if (hardErrors.length > 0) status = "hard_fail";
  else if (softErrors.length > 0) { status = "needs_human_review"; citation.requires_human_review = true; }

  // Augment with operational fields
  citation.parse_confidence = parseConfidence;
  citation.boundary_confidence = boundaryConfidence;
  citation.noise_score = noiseScore;
  citation.resident_count_detected = citation.affected_residents?.length || 0;
  citation.citation_has_state_reference = (citation.state_regulatory_references?.length || 0) > 0;
  citation.citation_has_direct_quote = (citation.direct_quotes?.length || 0) > 0;
  citation.validation_status = status;
  citation.hard_errors = hardErrors;
  citation.soft_errors = softErrors;

  return { hardErrors, softErrors, status };
}

function normalizeCitation(citation, surveyUid) {
  // Resolve tag — extraction stores as "tag", schema requires "tag_number"
  // Always normalize to tag_number regardless of which field it came in as
  const rawTag = (citation.tag_number || citation.tag || "").trim();
  // Pad 3-digit tags to 4 digits: F578 → F0578
  if (/^[FKE]\d{3}$/.test(rawTag)) {
    citation.tag_number = rawTag[0] + rawTag.slice(1).padStart(4, "0");
  } else {
    citation.tag_number = rawTag;
  }
  delete citation.tag; // remove legacy field
  // Ensure required arrays
  for (const f of ["cfr_citations","affected_residents","staff_statements","administrator_statements","facility_expectation_statements","policy_or_guideline_references","state_regulatory_references","direct_quotes","hard_errors","soft_errors"]) {
    if (!Array.isArray(citation[f])) citation[f] = [];
  }
  // Ensure required strings
  for (const f of ["scope_severity","scope_severity_raw","regulatory_title","federal_requirement_text","full_deficiency_text","deficiency_narrative_full","deficiency_summary","sample_scope_text","harm_or_risk_statement","findings_full","observation_evidence","interview_evidence","record_review_evidence","deficiency_category","department_owner"]) {
    if (typeof citation[f] !== "string") citation[f] = "";
  }
  // Ensure poc_inputs
  if (!citation.poc_inputs || typeof citation.poc_inputs !== "object") citation.poc_inputs = {};
  for (const f of ["immediate_correction_candidates","systemic_issue_candidates","monitoring_candidates","training_candidates","documentation_gaps","evidence_needed_for_rebuttal_or_context"]) {
    if (!Array.isArray(citation.poc_inputs[f])) citation.poc_inputs[f] = [];
  }
  for (const f of ["core_problem_statement","who_was_affected","what_failed","why_this_matters"]) {
    if (typeof citation.poc_inputs[f] !== "string") citation.poc_inputs[f] = "";
  }
  citation.survey_uid = surveyUid || "";
  citation.citation_uid = (citation.tag_number || "") + "-" + Date.now() + "-" + Math.random().toString(36).slice(2, 6);
  citation.requires_human_review = citation.requires_human_review || false;
  citation.page_start = citation.page_start || null;
  citation.page_end = citation.page_end || null;
  return citation;
}

// ─── PDF Parse — server-side extraction, column-aware ────────────────────────
app.post("/api/parse-pdf", async (req, res) => {
  const { pdfBase64, facilityName } = req.body;
  if (!pdfBase64) return res.status(400).json({ error: "pdfBase64 is required" });

  try {
    const pdfBuffer = Buffer.from(pdfBase64, "base64");

    // Custom page renderer — sorts items by Y then X to preserve column structure
    const pagerender = (pageData) => {
      return pageData.getTextContent({
        normalizeWhitespace: false,
        disableCombineTextItems: false,
      }).then((textContent) => {
        if (!textContent.items.length) return "";

        // Group items into lines using Y position (within 4pt = same line)
        const lineMap = {};
        for (const item of textContent.items) {
          if (!item.str || !item.str.trim()) continue;
          const y = Math.round(item.transform[5]);
          const bucket = Math.round(y / 4) * 4;
          if (!lineMap[bucket]) lineMap[bucket] = [];
          lineMap[bucket].push({ x: item.transform[4], str: item.str, width: item.width || 0 });
        }

        // Sort buckets top-to-bottom (PDF Y is inverted — higher Y = higher on page)
        const sortedBuckets = Object.keys(lineMap).map(Number).sort((a, b) => b - a);

        let text = "";
        let prevBucket = null;
        for (const bucket of sortedBuckets) {
          // Add extra blank line when large vertical gap — signals new citation section
          if (prevBucket !== null && prevBucket - bucket > 18) text += "\n";

          // Sort items left-to-right within each line
          const line = lineMap[bucket]
            .sort((a, b) => a.x - b.x)
            .map(w => w.str)
            .join(" ")
            .replace(/\s{3,}/g, "  ")
            .trim();

          if (line) text += line + "\n";
          prevBucket = bucket;
        }
        return text;
      });
    };

    const parsed = await pdfParse(pdfBuffer, { pagerender });
    let text = parsed.text || "";

    // Post-process — fix common CMS-2567 extraction issues
    text = text
      .replace(/([a-z])([A-Z])/g, "$1 $2")
      .replace(/([A-Z]{2,})([A-Z][a-z])/g, "$1 $2")
      .replace(/F(\d{3})/g, "\nF$1")
      .replace(/K(\d{3})/g, "\nK$1")
      .replace(/E(\d{3})/g, "\nE$1")
      .replace(/\n{4,}/g, "\n\n\n")
      .trim();

    console.log(`[PDF Parse] ${facilityName || "Unknown"} — ${parsed.numpages} pages, ${text.length} chars extracted`);

    return res.status(200).json({
      text,
      pages: parsed.numpages,
      chars: text.length,
    });
  } catch (err) {
    console.error("[PDF Parse] Error:", err.message);
    return res.status(500).json({ error: "PDF extraction failed: " + err.message });
  }
});

// ─── Survey Parse — server-side PDF parsing + AI extraction ─────────────────
// Runs entirely on Render — browser can navigate away, results polled by client
const parseJobs = new Map();

async function runParseJob(jobId, pdfBase64, facilityName) {
  const job = parseJobs.get(jobId);
  if (!job) return;

  try {
    // Step 1: Extract text from PDF using pdf-parse
    job.status = "extracting";
    const pdfBuffer = Buffer.from(pdfBase64, "base64");

    const pagerender = (pageData) => {
      return pageData.getTextContent({ normalizeWhitespace: false, disableCombineTextItems: false })
        .then((textContent) => {
          if (!textContent.items.length) return "";
          const lineMap = {};
          for (const item of textContent.items) {
            if (!item.str || !item.str.trim()) continue;
            const y = Math.round(item.transform[5]);
            const bucket = Math.round(y / 4) * 4;
            if (!lineMap[bucket]) lineMap[bucket] = [];
            lineMap[bucket].push({ x: item.transform[4], str: item.str });
          }
          const sortedBuckets = Object.keys(lineMap).map(Number).sort((a, b) => b - a);
          let text = "";
          let prevBucket = null;
          for (const bucket of sortedBuckets) {
            if (prevBucket !== null && prevBucket - bucket > 18) text += "\n";
            const line = lineMap[bucket].sort((a, b) => a.x - b.x).map(w => w.str).join(" ").replace(/\s{3,}/g, "  ").trim();
            if (line) text += line + "\n";
            prevBucket = bucket;
          }
          return text;
        });
    };

    const parsed = await pdfParse(pdfBuffer, { pagerender });
    let docText = (parsed.text || "")
      .replace(/([a-z])([A-Z])/g, "$1 $2")
      .replace(/([A-Z]{2,})([A-Z][a-z])/g, "$1 $2")
      .replace(/F(\d{3})/g, "\nF$1")
      .replace(/K(\d{3})/g, "\nK$1")
      .replace(/E(\d{3})/g, "\nE$1")
      .replace(/\n{4,}/g, "\n\n\n")
      .trim();

    job.pages = parsed.numpages;
    job.chars = docText.length;

    // Contingency 4: detect scanned/image-only PDFs
    // A real CMS-2567 should have substantial text. Under 200 chars/page = likely scanned.
    const charsPerPage = parsed.numpages > 0 ? docText.length / parsed.numpages : 0;
    if (docText.length < 500 || charsPerPage < 200) {
      job.status = "complete";
      job.result = {
        facility_name: facilityName, survey_date: null, survey_type: null, citations: [],
        scanned_document: true,
        error_hint: "This PDF appears to be a scanned image with no extractable text. Please use a text-based PDF exported from iQIES or your state survey system, or convert this document to a searchable PDF using Adobe Acrobat first."
      };
      console.warn("[Parse " + jobId + "] Scanned/image PDF detected — " + docText.length + " chars, " + charsPerPage.toFixed(0) + " chars/page");
      return;
    }

    job.status = "parsing";
    console.log("[Parse " + jobId + "] Extracted " + parsed.numpages + " pages, " + docText.length + " chars");

    // ══════════════════════════════════════════════════════════════════════════
    // STAGE 1 — DOCUMENT CLEANUP
    // Strip all CMS-2567 page furniture, headers, footers, boilerplate, and
    // continuation markers BEFORE any segmentation or extraction.
    // This prevents repeated form text from inflating citation blocks.
    // ══════════════════════════════════════════════════════════════════════════

    // LINE-BY-LINE pre-clean pass — exact match patterns from CMS-2567 spec
    // ══════════════════════════════════════════════════════════════════════════
    // STAGES 1-3: deterministic parsing via cms2567Parser + optional AI enrichment
    // cms2567Parser guarantees every tag surfaces, even if AI fails
    // ══════════════════════════════════════════════════════════════════════════



    // Build the AI enrichment function — called per citation by parseCMS2567
    // Verbatim extraction system prompt — definitive CMS-2567 spec
    const extractionSystemPrompt = `You are extracting Statements of Deficiencies from a CMS-2567.

==================================================
CRITICAL LEGAL REQUIREMENT — VERBATIM EXTRACTION
==================================================

All deficiency text MUST be captured VERBATIM.

DO NOT:
- summarize
- paraphrase
- rewrite
- shorten
- clean grammar
- interpret meaning
- remove repetition
- convert format

Return the exact wording as written by the surveyor.

==================================================
CORE STRUCTURE RULE — F-TAG BASED
==================================================

Each F-tag (F####) represents ONE deficiency citation.
A citation STARTS at the F-tag header line.
A citation ENDS only when a DIFFERENT F-tag begins.

==================================================
F-TAG ALIGNMENT RULE (CRITICAL)
==================================================

Each citation MUST begin exactly at the F-tag line.
The FIRST line of each extracted citation MUST contain the F-tag and SS = severity.
DO NOT start extraction mid-paragraph or attach content to the wrong F-tag.

==================================================
MULTI-PAGE CONTINUATION RULE
==================================================

If the same F-tag appears again OR text includes "Continued from page X":
→ this is the SAME citation — merge ALL text, ignore page boundaries.
NEVER stop extraction at page breaks.

==================================================
POC EXCLUSION RULE (MANDATORY)
==================================================

Extract ONLY deficiency text. DO NOT include Plan of Correction text.
Stop extraction BEFORE any of these appear:
- A. CORRECTIVE ACTION
- B. RESIDENTS AFFECTED
- C. SYSTEMIC CHANGES
- D. MONITORING
- EDUCATION/TRAINING
- corrective action language
- facility response language

==================================================
WHAT YOU MUST CAPTURE (VERBATIM)
==================================================

For each F-tag include EVERYTHING:
- F-tag line, SS = severity, regulatory title
- CFR(s)
- ALL federal regulation text before "This REQUIREMENT is NOT MET as evidenced by:"
- The phrase "This REQUIREMENT is NOT MET as evidenced by:"
- ALL narrative: Based on…, The facility failed to…, sample/scope, risk statements,
  Findings included…, ALL resident-level findings, ALL interviews, ALL observations,
  ALL record reviews, ALL staff statements, ALL administrator statements, ALL state references
END immediately BEFORE the next F-tag.

EXCLUDE ONLY: headers, footers, facility address blocks, column labels, signature sections.
F0000 Initial Comments: capture verbatim but store separately, NOT as a citation.
FAILSAFE: If unsure → INCLUDE text.

==================================================
OUTPUT FORMAT
==================================================

Return ONLY valid JSON — no markdown, no preamble:

{
  "initial_comments": { "tag_number": "F0000", "full_text": "VERBATIM TEXT" },
  "citations": [
    {
      "tag_number": "F####",
      "full_deficiency_text": "FULL VERBATIM DEFICIENCY TEXT",
      "scope_severity": "",
      "regulatory_title": "",
      "cfr_citations": [],
      "federal_requirement_text": "",
      "deficiency_narrative_full": "",
      "harm_or_risk_statement": "",
      "observation_evidence": "",
      "interview_evidence": "",
      "record_review_evidence": "",
      "staff_statements": [],
      "state_regulatory_references": [],
      "affected_residents": []
    }
  ]
}

FINAL PRINCIPLE: CMS-2567 DEFICIENCY = EVERYTHING BETWEEN F-TAGS (EXCLUDING POC).
VERBATIM TEXT IS SOURCE OF TRUTH.`;

    const aiExtractor = async ({ tag_number, raw_block, fallback }) => {
      // Send the complete raw block — no header/narrative splitting, no truncation
      // The CMS-2567 spec requires EVERYTHING between F-tags verbatim
      // raw_block is already the merged, continuation-page-joined full block
      const prompt = "Extract tag " + tag_number + " from this CMS-2567 block.\n\n" +
        "RULE: Capture ALL text VERBATIM. Do not summarize, rephrase, shorten, or skip any content.\n" +
        "RULE: Include everything from the F-tag line through the last line before the next F-tag.\n" +
        "RULE: full_deficiency_text must contain the COMPLETE verbatim block.\n\n" +
        "=== FULL CITATION BLOCK ===\n" + raw_block +
        "\n\nReturn a JSON object: {tag_number, full_deficiency_text (VERBATIM COMPLETE BLOCK), " +
        "scope_severity, regulatory_title, cfr_citations, federal_requirement_text, " +
        "deficiency_narrative_full, harm_or_risk_statement, observation_evidence, " +
        "interview_evidence, record_review_evidence, staff_statements, " +
        "state_regulatory_references, affected_residents}. Return ONLY valid JSON.";

      // Attempt 1
      let result = null;
      for (let attempt = 0; attempt < 2; attempt++) {
        try {
          const command = new InvokeModelCommand({
            modelId: BEDROCK_MODEL_ID, contentType: "application/json", accept: "application/json",
            body: JSON.stringify({ anthropic_version: "bedrock-2023-05-31", max_tokens: 8000, system: extractionSystemPrompt, messages: [{ role: "user", content: prompt }] }),
          });
          const resp = await client.send(command);
          const text = JSON.parse(new TextDecoder().decode(resp.body))?.content?.[0]?.text || "";
          const arrStart = text.indexOf("["), arrEnd = text.lastIndexOf("]");
          const objStart = text.indexOf("{"), objEnd = text.lastIndexOf("}");
          if (arrStart !== -1 && arrEnd !== -1) result = JSON.parse(text.slice(arrStart, arrEnd + 1))[0];
          else if (objStart !== -1 && objEnd !== -1) {
            const o = JSON.parse(text.slice(objStart, objEnd + 1));
            result = o.citations?.[0] || o;
          }
          if (result) break;
        } catch(e) {
          console.warn("[Parse " + jobId + "] " + tag_number + " AI attempt " + (attempt+1) + " failed:", e.message);
          if (attempt === 0) await new Promise(r => setTimeout(r, 1000));
        }
      }
      return result;
    };

    job.status = "parsing";
    job.totalChunks = 0; // will be updated once we know tag count

    // Check for cancellation before starting extraction
    if (job.status === "cancelled") {
      console.log("[Parse " + jobId + "] Cancelled before extraction");
      return;
    }

    if (job.disableEnrichment) {
      console.log("[Parse " + jobId + "] AI enrichment DISABLED by admin setting");
    }
    const parseResult = await parseCMS2567(docText, {
      aiExtractor: job.disableEnrichment ? null : aiExtractor,
      concurrency: 4,
    });

    job.totalChunks = parseResult.stats.candidate_count;
    job.currentChunk = parseResult.stats.candidate_count;

    console.log("[Parse " + jobId + "] parseCMS2567 stats:", JSON.stringify(parseResult.stats));

    const allCitations = parseResult.citations;
    const instrumentation = {
      raw_text_length: docText.length,
      candidate_tag_count: parseResult.stats.candidate_count,
      candidate_tags: allCitations.map(c => c.tag_number),
      llm_extracted_count: parseResult.stats.ai_count,
      fallback_count: parseResult.stats.fallback_count,
      validated_count: 0,
      rejected_count: 0,
      rejection_reasons: []
    };
    job.instrumentation = instrumentation;

    // ══════════════════════════════════════════════════════════════════════════
    // STAGE 4 — VALIDATE & NORMALIZE
    // Run every citation through the validator. Hard fails are flagged but kept
    // so the user can see them. Only approved_for_poc citations go to generation.
    // ══════════════════════════════════════════════════════════════════════════

    const surveyUid = jobId;
    let hardFailCount = 0, humanReviewCount = 0, approvedCount = 0;

    // Update instrumentation
    const extractedCount = allCitations.filter(Boolean).length;
    instrumentation.llm_extracted_count = extractedCount;
    console.log("[Parse " + jobId + "] LLM extracted: " + extractedCount + " of " + parseResult.stats.candidate_count + " tags");

    const validatedCitations = allCitations
      .filter(Boolean)
      .filter(c => {
        // Keep any citation that has a valid tag and some narrative content
        // Even imperfect citations surface to the user — never silently drop
        const tag = (c.tag || c.tag_number || "").trim();
        const narrative = (c.deficiency_narrative_full || c.deficiency_statement || "").trim();
        if (!tag || tag === "F0000" || tag === "F000") return false;
        if (!/^[FKE]\d{3,4}$/.test(tag)) return false;
        // Keep even if narrative is short — validator will soft-flag it
        return narrative.length >= 10 || true; // always keep if tag is valid
      })
      .map(c => {
        const normalized = normalizeCitation(c, surveyUid);
        const { status } = validateAndScoreCitation(normalized);
        if (status === "hard_fail") { hardFailCount++; instrumentation.rejection_reasons.push(normalized.tag_number + ": " + (normalized.hard_errors || []).join("; ")); }
        else if (status === "needs_human_review") humanReviewCount++;
        else approvedCount++;
        return normalized;
      });

    // Deduplicate: same tag + same first 80 chars of narrative = true duplicate
    const seen = new Set();
    const deduped = validatedCitations.filter(c => {
      const key = c.tag_number + "|" + (c.deficiency_narrative_full || "").slice(0, 80).trim();
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });

    instrumentation.validated_count = deduped.filter(c => c.validation_status !== "hard_fail").length;
    instrumentation.rejected_count = hardFailCount;

    console.log("[Parse " + jobId + "] Final: " + deduped.length + " citations | " +
      approvedCount + " approved | " + humanReviewCount + " need review | " + hardFailCount + " hard fails");
    console.log("[Parse " + jobId + "] Instrumentation:", JSON.stringify(instrumentation));

    // Extract facility/survey metadata from document header
    let facilityName2 = facilityName || "";
    let surveyDate2 = null;
    let surveyType2 = null;
    let surveyMetadata = {};
    try {
      // Use first 4000 chars for header extraction — covers multi-column layout
      const hText = docText.slice(0, 4000);
      const hLines = hText.split("\n");

      // ── Survey date — handles both MM/DD/YYYY and ISO YYYY-MM-DD ──────────
      // Date can be 200+ chars after the label in multi-column layout
      const dateMatchMMDD = hText.match(/DATE SURVEY COMPLETED[\s\S]{0,300}?(\d{2}\/\d{2}\/\d{4})/i);
      const dateMatchISO  = hText.match(/DATE SURVEY COMPLETED[\s\S]{0,300}?(\d{4}-\d{2}-\d{2})/i);
      let rawSurveyDate = "";
      if (dateMatchMMDD) {
        rawSurveyDate = dateMatchMMDD[1]; // MM/DD/YYYY
        const [m,d,y] = rawSurveyDate.split("/");
        surveyDate2 = y + "-" + m + "-" + d;
      } else if (dateMatchISO) {
        surveyDate2 = dateMatchISO[1]; // already ISO
        const [y,m,d] = surveyDate2.split("-");
        rawSurveyDate = m + "/" + d + "/" + y;
      }

      // ── CCN — appears after IDENTIFICATION NUMBER label (same line or next lines)
      const ccnMatch = hText.match(/IDENTIFICATION NUMBER[\s\S]{0,200}?\n\s*([0-9]{5,10})\s/i)
        || hText.match(/IDENTIFICATION NUMBER[^\n]{0,50}([0-9]{5,10})/i);
      const providerNumber = ccnMatch ? ccnMatch[1].trim() : "";

      // ── Event ID and Facility ID — footer of any page ─────────────────────
      const fullText5000 = docText.slice(0, 5000);
      const eventMatch = fullText5000.match(/Event ID[:\s]+([A-Z0-9\-]{4,20})/i);
      const eventId = eventMatch ? eventMatch[1].trim() : "";
      const facilIdMatch = fullText5000.match(/Facility ID[:\s]+([A-Z0-9\-]{4,20})/i);
      const facilityIdDoc = facilIdMatch ? facilIdMatch[1].trim() : "";

      // ── Facility name and address ─────────────────────────────────────────
      // CMS-2567 has two layouts:
      // Layout A (original CMS PDF): label line, then value on NEXT line
      //   Line N:   "NAME OF PROVIDER OR SUPPLIER"
      //   Line N+1: "BIRCH CREEK POST ACUTE & REHABILITATION"
      //
      // Layout B (pdftotext of printed/exported form): labels and values are
      //   on parallel lines separated by large whitespace gaps:
      //   Line N:   "Name of Facility Surveyed:            Facility Address (Street, City..."
      //   Line N+1: "BIRCH CREEK POST ACUTE & REHABILITATIO    601 S ORCHARD STREET, TACOMA..."
      //
      // Strategy: find the label line, then look at the NEXT line and split on
      // large whitespace to separate name (left) from address (right).
      let facilityAddress = "";
      for (let li = 0; li < hLines.length; li++) {
        const line = hLines[li];
        // Both layout A and B have this label somewhere
        if (/Name of (?:Facility Surveyed|Provider or Supplier)/i.test(line)) {
          const dataLine = (hLines[li+1] || "").trimEnd();
          if (dataLine.trim().length >= 3) {
            // Split on 4+ consecutive spaces — left part = name, right part = address
            const splitM = dataLine.match(/^(.+?)\s{4,}(.+)$/);
            if (splitM) {
              facilityName2 = splitM[1].trim();
              facilityAddress = splitM[2].trim();
            } else {
              facilityName2 = dataLine.trim();
            }
            break;
          }
        }
        // Layout A alternate: "NAME OF PROVIDER OR SUPPLIER" label
        if (/NAME OF PROVIDER OR SUPPLIER/i.test(line) && !facilityName2) {
          const next = (hLines[li+1]||"").trim();
          if (next.length >= 3 && !/^(ID|Prefix|Tag|SUMMARY|PLAN|Completion|STREET)/i.test(next)) {
            facilityName2 = next;
          }
        }
        // Layout A for address
        if (/STREET ADDRESS/i.test(line) && !facilityAddress) {
          const next = (hLines[li+1]||"").trim();
          if (/^[0-9]/.test(next) && next.length >= 8) facilityAddress = next;
        }
      }

      // ── Total pages ───────────────────────────────────────────────────────
      const pagesMatch = fullText5000.match(/Page\s+\d+\s+of\s+(\d+)/i);
      const totalPages = pagesMatch ? parseInt(pagesMatch[1]) : null;

      // ── Printed date ──────────────────────────────────────────────────────
      const printedMatch = hText.match(/PRINTED[:\s]+(\d{2}\/\d{2}\/\d{4})/i);
      const printedDate = printedMatch ? printedMatch[1] : "";

      surveyMetadata = {
        provider_name: facilityName2,
        provider_number: providerNumber,
        facility_id: facilityIdDoc,
        event_id: eventId,
        survey_completed_date: rawSurveyDate || surveyDate2 || "",
        facility_address: facilityAddress,
        total_pages: totalPages,
        printed_date: printedDate,
      };
    } catch(e) { console.warn("[Parse] Header extraction error:", e.message); }

    job.status = "complete";
    // Contingency 9: suggest vision fallback if many stubs or hard fails
    const stubCount = deduped.filter(c => c._stub).length;
    const visionFallbackSuggested = stubCount > 0 || hardFailCount > Math.floor(deduped.length * 0.3);

    job.result = {
      facility_name: facilityName2,
      survey_date: surveyDate2,
      survey_type: surveyType2,
      initial_comments: parseResult.initial_comments || "",
      survey_metadata: surveyMetadata,
      citations: deduped,
      validation_summary: {
        total: deduped.length,
        approved: approvedCount,
        needs_review: humanReviewCount,
        hard_fails: hardFailCount,
        stubs: stubCount,
        candidate_count: parseResult.stats.candidate_count
      },
      vision_fallback_suggested: visionFallbackSuggested,
      instrumentation
    };
    console.log("[Parse " + jobId + "] Complete — " + deduped.length + " citations");

  } catch(err) {
    console.error("[Parse " + jobId + "] Fatal:", err.message);
    job.status = "error";
    job.error = err.message;
  }
}

app.post("/api/parse/start", async (req, res) => {
  const { pdfBase64, facilityName, disableEnrichment } = req.body;
  if (!pdfBase64) return res.status(400).json({ error: "pdfBase64 is required" });

  const jobId = "parse-" + Date.now() + "-" + Math.random().toString(36).slice(2, 7);
  const job = { jobId, status: "queued", pages: 0, chars: 0, totalChunks: 0, currentChunk: 0, result: null, error: null, disableEnrichment: disableEnrichment === true };
  parseJobs.set(jobId, job);

  runParseJob(jobId, pdfBase64, facilityName).catch(e => {
    const j = parseJobs.get(jobId);
    if (j) { j.status = "error"; j.error = e.message; }
  });

  console.log("[Parse " + jobId + "] Started for " + (facilityName || "unknown facility"));
  return res.status(200).json({ jobId });
});

app.get("/api/parse/status/:jobId", (req, res) => {
  const job = parseJobs.get(req.params.jobId);
  if (!job) return res.status(404).json({ error: "Job not found" });
  return res.status(200).json({
    jobId: job.jobId,
    status: job.status,
    pages: job.pages,
    chars: job.chars,
    totalChunks: job.totalChunks,
    currentChunk: job.currentChunk,
    result: job.status === "complete" ? job.result : null,
    error: job.error || null,
  });
});

// ─── Parse Cancel ─────────────────────────────────────────────────────────────
app.post("/api/parse/cancel/:jobId", (req, res) => {
  const job = parseJobs.get(req.params.jobId);
  if (!job) return res.status(404).json({ error: "Job not found" });
  job.status = "cancelled";
  console.log("[Parse " + req.params.jobId + "] Cancelled by user");
  return res.status(200).json({ success: true });
});

// ─── Batch Generate — kick off server-side batch, return batchId immediately ──
app.post("/api/batch/start", async (req, res) => {
  const { citations, facility, facilityId, settings, userId } = req.body;
  if (!citations?.length || !facility || !facilityId) {
    return res.status(400).json({ error: "citations, facility, and facilityId are required" });
  }

  const batchId = "batch-" + Date.now() + "-" + Math.random().toString(36).slice(2, 8);
  const job = {
    batchId,
    status: "running",
    total: citations.length,
    done: 0,
    failed: 0,
    current: 0,
    errors: [],
    completedPips: [],
    startedAt: new Date().toISOString(),
    completedAt: null,
    facilityName: facility.facility_name,
  };

  batchJobs.set(batchId, job);

  // Fire and forget — runs in background, client polls for status
  runBatchJob(batchId, citations, facility, settings || {}, userId, facilityId)
    .catch(err => {
      console.error(`[Batch ${batchId}] Fatal error:`, err.message);
      const j = batchJobs.get(batchId);
      if (j) { j.status = "error"; j.errorMessage = err.message; }
    });

  console.log(`[Batch ${batchId}] Started — ${citations.length} citations for ${facility.facility_name}`);
  return res.status(200).json({ batchId, total: citations.length, status: "running" });
});

// ─── Batch Status — poll this every 5 seconds ─────────────────────────────────
app.get("/api/batch/status/:batchId", (req, res) => {
  const job = batchJobs.get(req.params.batchId);
  if (!job) return res.status(404).json({ error: "Batch not found" });
  return res.status(200).json({
    batchId: job.batchId,
    status: job.status,
    total: job.total,
    done: job.done,
    failed: job.failed,
    current: job.current,
    errors: job.errors,
    startedAt: job.startedAt,
    completedAt: job.completedAt,
    facilityName: job.facilityName,
    // Only return completed pips when batch is done — avoids sending partial data repeatedly
    completedPips: job.status === "complete" ? job.completedPips : [],
  });
});

// ─── Batch Cancel ─────────────────────────────────────────────────────────────
app.post("/api/batch/cancel/:batchId", (req, res) => {
  const job = batchJobs.get(req.params.batchId);
  if (!job) return res.status(404).json({ error: "Batch not found" });
  job.status = "cancelled";
  console.log(`[Batch ${job.batchId}] Cancelled by client`);
  return res.status(200).json({ success: true });
});

app.listen(PORT, () => console.log(`SurvAI API running on port ${PORT}`));

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
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SECRET_KEY  // service role — bypasses RLS for server writes
);

// ─── In-memory batch job store ────────────────────────────────────────────────
// Render Pro keeps the process alive — safe to store in memory between requests
const batchJobs = new Map();

// ─── POC Generation Helper ────────────────────────────────────────────────────
async function generatePOCOnServer(citation, facility, guidance, includeDates) {
  const systemPrompt = `You are a healthcare compliance specialist. Generate a complete Plan of Correction for a CMS deficiency citation. Return ONLY valid JSON, no markdown. Format: {"statement_of_deficiency":"","root_cause_analysis":"","immediate_corrective_actions":"","residents_affected":"","systemic_changes":"","education_and_training":"","policy_procedure_review":"","monitoring_and_auditing":"","sustainability_plan":"","projected_compliance_date":"","attestation":""}`;

  const dateInstruction = includeDates
    ? "Include specific calendar dates in corrective actions."
    : "Use relative timeframes only (e.g. 'within 30 days', 'immediately', 'ongoing monthly'). Do NOT include specific calendar dates in the narrative. The projected_compliance_date field should still be a valid YYYY-MM-DD date.";

  const userPrompt = `FACILITY: ${facility.facility_name} (${facility.facility_type || "SNF"}) | CCN: ${facility.facility_id || ""} | State: ${facility.state || ""}
TAG: ${citation.tags?.join(", ")} | Survey Date: ${citation.survey_date} | Scope/Severity: ${citation.scope_severity}
${citation.title ? "REGULATION: " + citation.title : ""}
${citation.cfr_citation ? "CFR: " + citation.cfr_citation : ""}
DEFICIENCY: ${citation.deficiency_statement || citation.deficiency_summary || ""}
${citation.deficiency_narrative_full ? "FULL NARRATIVE: " + citation.deficiency_narrative_full.slice(0, 800) : ""}
${citation.harm_or_risk_statement ? "HARM/RISK: " + citation.harm_or_risk_statement : ""}
${citation.supporting_observations || citation.observations ? "OBSERVATIONS: " + (citation.supporting_observations || citation.observations) : ""}
${citation.residents_affected || citation.resident_impact ? "RESIDENTS AFFECTED: " + (citation.residents_affected || citation.resident_impact) : ""}
${citation.deficiency_category ? "CATEGORY: " + citation.deficiency_category : ""}
${citation.department_owner ? "DEPARTMENT: " + citation.department_owner : ""}
${citation.poc_inputs?.what_failed ? "WHAT FAILED: " + citation.poc_inputs.what_failed : ""}
${citation.poc_inputs?.documentation_gaps?.length ? "DOCUMENTATION GAPS: " + citation.poc_inputs.documentation_gaps.join("; ") : ""}
Compliance Date: ${citation.projected_compliance_date || "10 days from survey date"}
${guidance ? "GUIDANCE: " + guidance.slice(0, 1500) : ""}
${dateInstruction}
Generate a complete, CMS-acceptable Plan of Correction addressing this specific deficiency. Return ONLY the JSON object.`;

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

  const CONCURRENCY = 4;
  const queue = [...citations.map((c, i) => ({ ...c, _idx: i }))];
  const results = new Array(citations.length).fill(null);

  const processOne = async (item) => {
    const { _idx, ...citation } = item;
    let pipData = null;
    let lastError = null;

    for (let attempt = 0; attempt < 2; attempt++) {
      try {
        const guidance = ""; // guidance lookup happens client-side; server uses deficiency text
        pipData = await generatePOCOnServer(citation, facility, guidance, settings.includeDates !== false);
        break;
      } catch (err) {
        lastError = err;
        if (attempt === 0) await new Promise(r => setTimeout(r, 1500));
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
        citation_data: citation,
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
          signers: [],
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
        console.warn(`[Batch ${batchId}] Supabase save failed for ${citation.tags?.join(",")}: ${dbErr.message}`);
      }

      results[_idx] = newPip;
      job.done++;
    } else {
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
    job.status = "parsing";
    console.log("[Parse " + jobId + "] Extracted " + parsed.numpages + " pages, " + docText.length + " chars");

    // ── Step 2: Regex-first — locate every F/K/E tag deterministically ─────────
    // Tags are the primary delimiter. Regex never misses or invents a tag.
    // Skip F0000 (initial comments metadata — not a deficiency citation)
    const tagPattern = /\b([FKE])(\d{3,4})\b/g;
    const tagMatches = [];
    let m;
    while ((m = tagPattern.exec(docText)) !== null) {
      const tag = m[1] + m[2];
      if (tag === "F0000" || tag === "F000") continue; // skip initial comments block
      tagMatches.push({ tag, pos: m.index });
    }

    // Deduplicate: same tag within 100 chars = same occurrence (tag printed in header + body)
    const uniqueTagPositions = tagMatches.filter((t, i) => {
      if (i === 0) return true;
      const prev = tagMatches[i - 1];
      return !(t.tag === prev.tag && t.pos - prev.pos < 100);
    });

    console.log("[Parse " + jobId + "] Regex found " + uniqueTagPositions.length + " tags: " + uniqueTagPositions.map(t => t.tag).join(", "));
    job.totalChunks = uniqueTagPositions.length;

    if (uniqueTagPositions.length === 0) {
      job.status = "complete";
      job.result = { facility_name: facilityName, survey_date: null, survey_type: null, citations: [] };
      console.warn("[Parse " + jobId + "] No tags found in document");
      return;
    }

    // ── Step 3: Slice document into per-citation blocks ───────────────────────
    // Each block = everything from this tag's position to the next tag's position
    // "Continued from page X" text stays inside the same block — not a new citation
    const tagBlocks = uniqueTagPositions.map((t, i) => {
      const end = i + 1 < uniqueTagPositions.length
        ? uniqueTagPositions[i + 1].pos
        : docText.length;
      const blockText = docText.slice(t.pos, end)
        .replace(/Continued from page \d+/gi, "") // strip continuation headers
        .trim();
      return { tag: t.tag, text: blockText };
    });

    // ── Step 4: Extract header info ───────────────────────────────────────────
    let facilityName2 = facilityName, surveyDate2 = null, surveyType2 = null;
    try {
      const headerText = docText.slice(0, 3000);
      const hCmd = new InvokeModelCommand({
        modelId: BEDROCK_MODEL_ID,
        contentType: "application/json",
        accept: "application/json",
        body: JSON.stringify({
          anthropic_version: "bedrock-2023-05-31",
          max_tokens: 300,
          messages: [{ role: "user", content:
            "Extract the facility name, survey completion date (YYYY-MM-DD), and survey type from this CMS-2567 header. " +
            "Return ONLY valid JSON with keys: facility_name, survey_date, survey_type.\n\n" + headerText
          }]
        }),
      });
      const hResp = await client.send(hCmd);
      const hData = JSON.parse(new TextDecoder().decode(hResp.body));
      const hText = hData?.content?.[0]?.text || "";
      const hS = hText.indexOf("{"), hE = hText.lastIndexOf("}");
      if (hS !== -1 && hE !== -1) {
        const h = JSON.parse(hText.slice(hS, hE + 1));
        if (h.facility_name) facilityName2 = h.facility_name;
        if (h.survey_date) surveyDate2 = h.survey_date;
        if (h.survey_type) surveyType2 = h.survey_type;
      }
    } catch(e) { console.warn("[Parse " + jobId + "] Header extract failed:", e.message); }

    // ── Step 5: AI fills in details for each confirmed tag block ─────────────
    // Process in batches of 4 tags per AI call to minimize round trips
    const BATCH_SIZE = 4;
    const allCitations = [];

    const systemPrompt = `You are a regulatory document extraction engine specialized in CMS-2567 nursing facility survey documents.

Your job is to extract every deficiency citation from a CMS-2567 Statement of Deficiencies and convert it into clean, structured JSON for downstream compliance workflows including plan-of-correction generation, citation clustering, root-cause analysis, and reporting.

IMPORTANT PRINCIPLES:
1. Treat the CMS-2567 as a structured regulatory artifact, not plain narrative text.
2. A new citation block begins when a new ID Prefix Tag (F####) appears.
3. A citation may span multiple pages. "Continued from page X" does NOT begin a new citation.
4. Stop a citation only when the next new deficiency tag begins.
5. Ignore F0000 Initial Comments — survey metadata, not a deficiency.
6. Ignore page headers, footers, form labels, continuation boilerplate, and blank plan-of-correction columns.
7. Preserve factual accuracy. Do not infer facts not clearly stated in the document.

FOR EACH CITATION EXTRACT:
- tag_number: the F-tag normalized to F#### format
- scope_severity: the single letter after "SS =" (e.g. D, E, G)
- scope_severity_raw: exact text such as "SS = D"
- regulatory_title: short title immediately following the tag
- cfr_citations: array of all CFR references (e.g. ["483.10(c)(6)", "483.10(g)(12)"])
- federal_requirement_text: regulatory requirement language BEFORE "This REQUIREMENT is NOT MET"
- deficiency_summary: the opening surveyor summary of the failed practice (1-3 sentences)
- deficiency_narrative_full: all text from "This REQUIREMENT is NOT MET as evidenced by:" to end of citation
- sample_scope_text: phrases like "for 2 of 3 sampled residents"
- harm_or_risk_statement: surveyor's explicit risk or harm language
- observation_evidence: what surveyors directly observed
- interview_evidence: what staff or residents said during interviews
- record_review_evidence: findings from chart/record review
- affected_residents: array of objects, one per resident with fields: resident_id, citation_specific_issue, dates_mentioned, diagnoses_or_conditions, related_staff
- staff_statements: array of concise extracted staff statements
- state_regulatory_references: array of state regulation references (e.g. WAC citations)
- direct_quotes: array of verbatim quotes from residents or staff
- poc_inputs: object with fields: core_problem_statement, what_failed, who_was_affected, why_this_matters, immediate_correction_candidates (array), systemic_issue_candidates (array), monitoring_candidates (array), training_candidates (array), documentation_gaps (array)

DEFICIENCY CATEGORY — classify into one of: Resident Rights, Advance Directives, Beneficiary Notification, Environment of Care, Medication Management, Abuse and Neglect Investigation, MDS/Assessment Accuracy, Infection Prevention, Accident Prevention, Quality of Care, Pharmacy Services, Administration/Governance
- deficiency_category: one of the above
- department_owner: one of: Nursing, Social Services, MDS, Maintenance/Environmental, Administration, Rehabilitation, Medical Records, Pharmacy/Medication Systems, Interdisciplinary Team

EXTRACTION LOGIC:
- Everything BEFORE "This REQUIREMENT is NOT MET as evidenced by:" = regulation header
- Everything AFTER that phrase = deficiency narrative
- Create one affected_residents entry per named resident
- Capture dates exactly as written
- Capture direct quotes exactly as written
- Keep observations, interviews, and record reviews in separate fields

Return ONLY a valid JSON array of citation objects. No markdown. No explanation. Nothing before [ or after ].`;

    for (let bi = 0; bi < tagBlocks.length; bi += BATCH_SIZE) {
      job.currentChunk = bi + 1;
      const batch = tagBlocks.slice(bi, bi + BATCH_SIZE);

      const batchPrompt = "Extract all deficiency citation details for each of these " + batch.length +
        " CMS-2567 citation blocks. Include resident-level details, POC-ready fields, and all evidence types.\n\n" +
        batch.map((b, i) =>
          "=== CITATION BLOCK " + (i+1) + " | TAG: " + b.tag + " ===\n" + b.text.slice(0, 4000)
        ).join("\n\n") +
        "\n\nRequirements:" +
        "\n- Exclude F0000 initial comments" +
        "\n- Separate regulation text from deficiency narrative at 'This REQUIREMENT is NOT MET as evidenced by:'" +
        "\n- Include resident-level details where available" +
        "\n- Include state references when present" +
        "\n- Add POC-ready fields in poc_inputs" +
        "\n- Return a JSON array with exactly " + batch.length + " objects in order" +
        "\n- Return ONLY the JSON array, nothing else";

      try {
        const command = new InvokeModelCommand({
          modelId: BEDROCK_MODEL_ID,
          contentType: "application/json",
          accept: "application/json",
          body: JSON.stringify({
            anthropic_version: "bedrock-2023-05-31",
            max_tokens: 16000,
            system: systemPrompt,
            messages: [{ role: "user", content: batchPrompt }],
          }),
        });
        const resp = await client.send(command);
        const data = JSON.parse(new TextDecoder().decode(resp.body));
        const text = data?.content?.[0]?.text || "";
        const arrStart = text.indexOf("[");
        const arrEnd = text.lastIndexOf("]");
        if (arrStart !== -1 && arrEnd !== -1) {
          const batchResults = JSON.parse(text.slice(arrStart, arrEnd + 1));
          batch.forEach((b, i) => {
            const d = batchResults[i] || {};
            allCitations.push({
              tag: b.tag,                              // always from regex — never overridden by AI
              scope_severity: d.scope_severity || d.scope_severity_raw || "",
              title: d.regulatory_title || d.title || "",
              cfr_citation: Array.isArray(d.cfr_citations) ? d.cfr_citations.join(", ") : (d.cfr_citation || ""),
              deficiency_statement: d.deficiency_summary || d.deficiency_statement || "",
              observations: [d.observation_evidence, d.interview_evidence, d.record_review_evidence].filter(Boolean).join(" | ").slice(0, 500) || d.observations || "",
              residents_affected: Array.isArray(d.affected_residents)
                ? d.affected_residents.map(r => r.resident_id || r).filter(Boolean).join(", ")
                : (d.residents_affected || ""),
              deficiency_narrative_full: d.deficiency_narrative_full || "",
              harm_or_risk_statement: d.harm_or_risk_statement || "",
              deficiency_category: d.deficiency_category || "",
              department_owner: d.department_owner || "",
              cfr_citations: d.cfr_citations || [],
              affected_residents_detail: d.affected_residents || [],
              staff_statements: d.staff_statements || [],
              state_regulatory_references: d.state_regulatory_references || [],
              direct_quotes: d.direct_quotes || [],
              poc_inputs: d.poc_inputs || {},
            });
          });
          console.log("[Parse " + jobId + "] Batch " + Math.ceil((bi+1)/BATCH_SIZE) + "/" + Math.ceil(tagBlocks.length/BATCH_SIZE) + " — " + batch.length + " tags filled");
        } else {
          // AI returned bad JSON — still include tags with empty details so count stays correct
          batch.forEach(b => allCitations.push({ tag: b.tag, scope_severity: "", title: "", cfr_citation: "", deficiency_statement: "", observations: "", residents_affected: "", deficiency_narrative_full: "", harm_or_risk_statement: "", deficiency_category: "", department_owner: "", cfr_citations: [], affected_residents_detail: [], staff_statements: [], state_regulatory_references: [], direct_quotes: [], poc_inputs: {} }));
          console.warn("[Parse " + jobId + "] Batch " + Math.ceil((bi+1)/BATCH_SIZE) + " — invalid JSON from AI, using empty details");
        }
      } catch(e) {
        console.warn("[Parse " + jobId + "] Batch " + Math.ceil((bi+1)/BATCH_SIZE) + " failed:", e.message);
        batch.forEach(b => allCitations.push({ tag: b.tag, scope_severity: "", title: "", cfr_citation: "", deficiency_statement: "", observations: "", residents_affected: "", deficiency_narrative_full: "", harm_or_risk_statement: "", deficiency_category: "", department_owner: "", cfr_citations: [], affected_residents_detail: [], staff_statements: [], state_regulatory_references: [], direct_quotes: [], poc_inputs: {} }));
      }
    }

    job.status = "complete";
    job.result = { facility_name: facilityName2, survey_date: surveyDate2, survey_type: surveyType2, citations: allCitations };
    console.log("[Parse " + jobId + "] Complete — " + allCitations.length + " citations (deterministic)");

  } catch(err) {
    console.error("[Parse " + jobId + "] Fatal:", err.message);
    job.status = "error";
    job.error = err.message;
  }
}

app.post("/api/parse/start", async (req, res) => {
  const { pdfBase64, facilityName } = req.body;
  if (!pdfBase64) return res.status(400).json({ error: "pdfBase64 is required" });

  const jobId = "parse-" + Date.now() + "-" + Math.random().toString(36).slice(2, 7);
  const job = { jobId, status: "queued", pages: 0, chars: 0, totalChunks: 0, currentChunk: 0, result: null, error: null };
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

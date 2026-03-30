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

    // POC Gatekeeper — block generation for hard fails or very noisy citations
    const gate = canGeneratePOC(citation);
    if (!gate.allowed) {
      console.warn("[Batch " + batchId + "] Skipping " + (citation.tags?.[0] || "?") + " — " + gate.reason);
      job.failed++;
      job.errors.push((citation.tags?.[0] || "?") + ": blocked — " + gate.reason);
      job.current = job.done + job.failed;
      return;
    }
    if (gate.flagged) {
      console.log("[Batch " + batchId + "] " + (citation.tags?.[0] || "?") + " flagged (needs review) but proceeding");
    }

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

  // ── HARD fail rules — only truly fatal issues ────────────────────────────
  // Per architecture guidance: never let strict validation hide citations.
  // Visible ≠ approved. Keep the citation visible, flag it for review.
  if (tag === "F0000") hardErrors.push("F0000 must not appear as a deficiency citation");
  if (!/^[FKE]\d{3,4}$/.test(tag)) hardErrors.push("Invalid tag_number: " + tag);
  const ps = citation.page_start, pe = citation.page_end;
  if (ps !== null && pe !== null && typeof ps === "number" && typeof pe === "number" && ps > pe) hardErrors.push("page_start > page_end");

  // Only hard-fail on empty narrative if the block is truly empty (not just short)
  const narrative = (citation.deficiency_narrative_full || citation.deficiency_statement || "").trim();
  if (!narrative) hardErrors.push(tag + ": completely empty narrative");
  else if (narrative.length < 40) hardErrors.push(tag + ": narrative too short to be a real citation");

  // Auto-strip boilerplate from narrative fields — do not hard-fail, just clean
  const narrativeFields = ["deficiency_narrative_full","deficiency_summary","findings_full","observation_evidence","interview_evidence","record_review_evidence"];
  for (const field of narrativeFields) {
    if (containsBoilerplate(citation[field] || "")) {
      // Strip and flag for review — do not hard-fail
      citation[field] = stripBoilerplateFromField(citation[field]);
      softErrors.push(tag + ": boilerplate auto-stripped from " + field);
    }
  }

  // ── SOFT fail rules — flag for human review but keep the citation ─────────
  // These were previously hard errors — now demoted per architecture guidance
  if (!citation.regulatory_title?.trim()) softErrors.push(tag + ": missing regulatory_title");
  if (!citation.scope_severity?.trim()) softErrors.push(tag + ": missing scope_severity");
  if (!citation.cfr_citations?.length) softErrors.push(tag + ": missing CFR citations");
  if (ps === null || pe === null) softErrors.push(tag + ": missing page range");
  if ((citation.deficiency_summary || "").trim().length < 20) softErrors.push(tag + ": deficiency_summary short");
  if (/Continued from page/i.test(citation.deficiency_narrative_full || "")) softErrors.push(tag + ": continuation marker in narrative");
  const hasQuote = /"/.test(citation.deficiency_narrative_full || "") || /\u201c/.test(citation.deficiency_narrative_full || "");
  if (hasQuote && !(citation.direct_quotes?.length)) softErrors.push(tag + ": quotes in text but direct_quotes empty");
  const evidenceAllBlank = !citation.observation_evidence && !citation.interview_evidence && !citation.record_review_evidence;
  if (evidenceAllBlank && (citation.affected_residents?.length || 0) > 0) softErrors.push(tag + ": residents cited but evidence fields empty");
  const statePattern = /WAC\s+\d|RCW\s+\d|CMR\s+\d|\d{1,3}\s+ILCS/i;
  if (statePattern.test(citation.deficiency_narrative_full || "") && !(citation.state_regulatory_references?.length)) softErrors.push(tag + ": state reference detected but not extracted");

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
  for (const f of ["scope_severity","scope_severity_raw","regulatory_title","federal_requirement_text","deficiency_narrative_full","deficiency_summary","sample_scope_text","harm_or_risk_statement","findings_full","observation_evidence","interview_evidence","record_review_evidence","deficiency_category","department_owner"]) {
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

// ─── POC Gatekeeper ──────────────────────────────────────────────────────────
// A citation must pass all of these before server-side POC generation runs.
// Prevents bad downstream outputs from noisy or incomplete extractions.

function canGeneratePOC(citation) {
  if (!citation) return { allowed: false, reason: "No citation object" };
  if (citation.validation_status === "hard_fail") {
    return { allowed: false, reason: "Hard validation failure: " + (citation.hard_errors || []).join("; ") };
  }
  if ((citation.noise_score || 0) > 0.35) {
    return { allowed: false, reason: "Noise score too high: " + citation.noise_score };
  }
  if ((citation.boundary_confidence || 1) < 0.5) {
    return { allowed: false, reason: "Boundary confidence too low: " + citation.boundary_confidence };
  }
  if (!citation.deficiency_narrative_full?.trim() && !citation.deficiency_statement?.trim()) {
    return { allowed: false, reason: "No deficiency narrative — cannot generate meaningful POC" };
  }
  // Needs human review is allowed but flagged
  if (citation.requires_human_review || citation.validation_status === "needs_human_review") {
    return { allowed: true, flagged: true, reason: "Needs human review: " + (citation.soft_errors || []).join("; ") };
  }
  return { allowed: true, flagged: false, reason: null };
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
    job.status = "parsing";
    console.log("[Parse " + jobId + "] Extracted " + parsed.numpages + " pages, " + docText.length + " chars");

    // ══════════════════════════════════════════════════════════════════════════
    // STAGE 1 — DOCUMENT CLEANUP
    // Strip all CMS-2567 page furniture, headers, footers, boilerplate, and
    // continuation markers BEFORE any segmentation or extraction.
    // This prevents repeated form text from inflating citation blocks.
    // ══════════════════════════════════════════════════════════════════════════

    // LINE-BY-LINE pre-clean pass — exact match patterns from CMS-2567 spec
    // ── SAFE pre-strip patterns only — never strip citation structure ──────────
    // Do NOT strip: F-tags, SS=, regulatory titles, CFR lines,
    // "Continued from page" (needed for segmentation stitching),
    // or anything that could be part of a citation block.
    const LINE_EXCLUSION_PATTERNS = [
      /^PRINTED:\s*$/i,
      /^DEPARTMENT OF HEALTH AND HUMAN SERVICES\s*$/i,
      /^CENTERS FOR MEDICARE (AND|&) MEDICAID SERVICES\s*$/i,
      /^FORM APPROVED\s*$/i,
      /^OMB\s*N[O0]\..*$/i,
      /^STATEMENT OF DEFICIENCIES\s*$/i,
      /^AND PLAN OF CORRECTIONS?\s*$/i,
      /^FORM\s+CMS[-\s]?2567.*$/i,
      /^If continuation sheet Page\s+\d+\s+of\s+\d+\s*$/i,
      /^STATE REPRESENTATIVE SIGNATURE\s*$/i,
      /^SURVEYOR SIGNATURE\s*$/i,
      /^TITLE\s+DATE\s+TIME\s*$/i,
      /^\d+$/, // standalone page numbers only
    ];

    // INLINE patterns — only truly safe global replacements
    const INLINE_EXCLUSION_PATTERNS = [
      /DEPARTMENT OF HEALTH AND HUMAN SERVICES/gi,
      /CENTERS FOR MEDICARE (AND|&) MEDICAID SERVICES/gi,
      /FORM\s+CMS[-\s]?2567[^\n]*/gi,
      /Previous Versions? Obsolete[^\n]*/gi,
      /OMB\s*N[O0]\.?\s*0938-0391/gi,
      /FORM APPROVED/gi,
    ];
    // NOTE: "Continued from page X" is NOT stripped here — segmentation uses it
    // to know a citation is continuing. Strip it AFTER blocks are formed.

    // F0000 block — remove everything from F0000 up to the first real citation tag
    const f0000Match = docText.match(/\bF0*000\b[\s\S]*?(?=\bF0*[1-9]\d{2,3}\b)/);
    let workDoc = f0000Match ? docText.slice(f0000Match.index + f0000Match[0].length) : docText;

    // Apply inline patterns first
    for (const p of INLINE_EXCLUSION_PATTERNS) {
      workDoc = workDoc.replace(p, " ");
    }

    // Apply line-level patterns
    let cleanedText = workDoc
      .split("\n")
      .filter(line => {
        const t = line.trim();
        if (!t) return false;
        // Test each line-exclusion pattern
        for (const p of LINE_EXCLUSION_PATTERNS) {
          if (p.test(t)) return false;
        }
        // Discard short all-caps lines that are clearly column labels or metadata
        // PRESERVE: lines with F-tags, SS=, CFR, 483, WAC/RCW, §, or regulatory terms
        if (
          t === t.toUpperCase() &&
          t.length < 60 &&
          t.split(/\s+/).length <= 6 &&
          !/\b[FKE]\d{3,4}\b/.test(t) &&
          !/SS\s*=/.test(t) &&
          !/CFR|483\./.test(t) &&
          !/WAC|RCW|CMR/.test(t) &&
          !/§/.test(t) &&
          !/NOT MET/i.test(t)
        ) return false;
        return true;
      })
      .join("\n")
      .replace(/\n{3,}/g, "\n\n")
      .trim();

    const removedChars = docText.length - cleanedText.length;
    console.log("[Parse " + jobId + "] Stage 1 cleanup: " + cleanedText.length + " chars (" + removedChars + " removed)");

    // ══════════════════════════════════════════════════════════════════════════
    // STAGE 2 — CITATION SEGMENTATION
    // Find the FIRST occurrence of each unique F/K/E tag in the cleaned text.
    // A citation starts only when a new tag appears followed by regulatory
    // context (SS=, CFR, or regulatory title text).
    // ══════════════════════════════════════════════════════════════════════════

    // ── Find ALL F/K/E tag occurrences ───────────────────────────────────────
    // Collect every occurrence, then deduplicate to first occurrence per tag.
    // We do NOT gate on proximity here — Stage 1 cleanup may have spread
    // SS= and CFR text further than 300 chars from the tag.
    const tagPattern = /\b([FKE])(\d{3,4})\b/g;
    const allTagMatches = [];
    let m;
    while ((m = tagPattern.exec(cleanedText)) !== null) {
      const tag = m[1] + m[2];
      if (tag === "F0000" || tag === "F000") continue;
      allTagMatches.push({ tag, pos: m.index });
    }

    // Keep only FIRST occurrence of each tag
    const seenTags = new Set();
    const uniqueTagPositions = [];
    for (const t of allTagMatches) {
      if (seenTags.has(t.tag)) continue;
      seenTags.add(t.tag);
      uniqueTagPositions.push(t);
    }

    // Secondary filter: remove tags that are clearly cross-references, not citation starts.
    // A cross-reference tag sits inside a sentence (surrounded by word chars or "per"/"see"/"under").
    // A citation start tag sits at the beginning of a line or is preceded by whitespace only.
    const filteredTagPositions = uniqueTagPositions.filter((t, idx) => {
      // Always keep if it's the only tag or the first one
      if (uniqueTagPositions.length <= 1 || idx === 0) return true;
      // Check ~600 chars after the tag for citation markers (wider window post-cleanup)
      const after = cleanedText.slice(t.pos, t.pos + 400); // tag header is ~3-4 lines post-cleanup
      const hasCitationMarker =
        /SS\s*=\s*[A-L]/i.test(after) ||
        /42\s*CFR/i.test(after) ||
        /483\.\d/i.test(after) ||
        /This REQUIREMENT/i.test(after) ||
        /NOT MET/i.test(after) ||
        /deficien/i.test(after.slice(0, 200));
      // Check 50 chars BEFORE — cross-refs are preceded by "per", "see", "under", "F-tag", etc.
      const before = cleanedText.slice(Math.max(0, t.pos - 50), t.pos);
      const likelyCrossRef =
        /\b(per|see|under|reference|tag|per F|at F|to F|with F|from F)\s*$/i.test(before.trim());
      if (likelyCrossRef && !hasCitationMarker) return false;
      // If no citation marker found at all, still keep it — better to include than miss a citation
      return true;
    });

    const workingText = cleanedText;

    const instrumentation = {
      raw_text_length: docText.length,
      cleaned_text_length: cleanedText.length,
      candidate_tag_count: filteredTagPositions.length,
      candidate_tags: filteredTagPositions.map(t => t.tag),
      llm_extracted_count: 0,
      validated_count: 0,
      rejected_count: 0,
      rejection_reasons: []
    };
    job.instrumentation = instrumentation;

    console.log("[Parse " + jobId + "] Instrumentation: raw=" + docText.length +
      " cleaned=" + cleanedText.length +
      " all_tags=" + allTagMatches.length +
      " unique=" + uniqueTagPositions.length +
      " candidates=" + filteredTagPositions.length +
      " (" + filteredTagPositions.map(t => t.tag).join(", ") + ")");
    job.totalChunks = filteredTagPositions.length;

    if (filteredTagPositions.length === 0) {
      // Fallback: if filtering removed everything, use all unique positions
      // Better to over-include than return zero citations
      console.warn("[Parse " + jobId + "] Cross-ref filter removed all tags — falling back to all unique positions");
      filteredTagPositions.push(...uniqueTagPositions);
    }

    if (filteredTagPositions.length === 0) {
      // Last resort: return raw candidate blocks so user sees something
      // Better than "0 citations" when the document clearly has content
      console.warn("[Parse " + jobId + "] No tags found — returning empty result");
      job.status = "complete";
      job.result = {
        facility_name: facilityName, survey_date: null, survey_type: null, citations: [],
        instrumentation, candidate_fallback: true
      };
      return;
    }

    // ── Step 3: Slice document into per-citation blocks ───────────────────────
    // Each block = everything from this tag's position to the next tag's position
    // "Continued from page X" text stays inside the same block — not a new citation
    // ══════════════════════════════════════════════════════════════════════════
    // STAGE 3 — STRUCTURED EXTRACTION (per citation block)
    // Slice cleaned text by citation boundaries, then send to AI for
    // structured field extraction.
    // ══════════════════════════════════════════════════════════════════════════

    const tagBlocks = filteredTagPositions.map((t, i) => {
      const end = i + 1 < filteredTagPositions.length
        ? filteredTagPositions[i + 1].pos
        : workingText.length;
      // Slice from cleaned text — already stripped of boilerplate
      // Strip "Continued from page X" INSIDE the block — after boundary is established
      const blockText = workingText.slice(t.pos, end)
        .replace(/Continued from page\s+\d+/gi, " ")
        .replace(/\n{3,}/g, "\n\n")
        .trim();
      return { tag: t.tag, text: blockText };
    });

    // ── Step 4: Extract header info ───────────────────────────────────────────
    let facilityName2 = facilityName, surveyDate2 = null, surveyType2 = null;
    try {
      const headerText = workingText.slice(0, 3000);
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

    // ── Steps 4+5: Header + AI extraction — parallel ────────────────────────────
    // Fire header extraction immediately — runs in parallel with citation batches
    const headerPromise = (async () => {
      try {
        const headerText = workingText.slice(0, 2000);
        const hCmd = new InvokeModelCommand({
          modelId: BEDROCK_MODEL_ID, contentType: "application/json", accept: "application/json",
          body: JSON.stringify({ anthropic_version: "bedrock-2023-05-31", max_tokens: 200,
            messages: [{ role: "user", content: "From this CMS-2567 header extract facility_name, survey_date (YYYY-MM-DD), survey_type. Return ONLY valid JSON, nothing else.\n\n" + headerText }]
          }),
        });
        const hResp = await client.send(hCmd);
        const hText = JSON.parse(new TextDecoder().decode(hResp.body))?.content?.[0]?.text || "";
        const hS = hText.indexOf("{"), hE = hText.lastIndexOf("}");
        if (hS !== -1 && hE !== -1) {
          const h = JSON.parse(hText.slice(hS, hE + 1));
          if (h.facility_name) facilityName2 = h.facility_name;
          if (h.survey_date) surveyDate2 = h.survey_date;
          if (h.survey_type) surveyType2 = h.survey_type;
        }
      } catch(e) { console.warn("[Parse " + jobId + "] Header extract failed:", e.message); }
    })();

    const BATCH_SIZE = 8;   // 8 tags per call — fewer round trips vs 4
    const CONCURRENCY = 3;  // 3 batches in parallel — Bedrock handles concurrent requests fine
    const allCitations = new Array(tagBlocks.length).fill(null);

    const systemPrompt = `You are a CMS-2567 extraction engine built for high-precision regulatory parsing.

Your sole task is to extract true deficiency citation content from a CMS-2567 Statement of Deficiencies while aggressively excluding page furniture, repeated form text, and non-substantive boilerplate.

The text you receive has already been pre-cleaned. However, some boilerplate may still remain due to PDF extraction artifacts. Apply all exclusion rules strictly.

STEP 1 — CLASSIFY EVERY LINE OR BLOCK before extracting:
1. HEADER — agency names, form titles, OMB lines → DISCARD
2. FACILITY_METADATA — provider numbers, facility name/address blocks → DISCARD
3. COLUMN_LABEL — ID PREFIX TAG, SUMMARY STATEMENT, PROVIDER'S PLAN, COMPLETION DATE → DISCARD
4. CONTINUATION_MARKER — "Continued from page X" → DISCARD
5. FOOTER — FORM CMS-2567, Event ID, Facility ID, Page X of Y → DISCARD
6. SURVEY_METADATA — F0000 INITIAL COMMENTS → DISCARD
7. CITATION_HEADER — F-tag + SS= + regulatory title → KEEP
8. REGULATION_TEXT — CFR citations + federal requirement language → KEEP
9. DEFICIENCY_NARRATIVE — findings, residents, observations, interviews → KEEP
10. PLAN_OF_CORRECTION_TEXT — only if provider actually entered text → KEEP
11. SIGNATURE_BLOCK → DISCARD

NON-NEGOTIABLE EXCLUSIONS — never include in any output field:
DEPARTMENT OF HEALTH AND HUMAN SERVICES | CENTERS FOR MEDICARE & MEDICAID SERVICES |
STATEMENT OF DEFICIENCIES AND PLAN OF CORRECTIONS | FORM APPROVED | OMB NO. 0938-0391 |
PROVIDER/SUPPLIER/CLIA IDENTIFICATION NUMBER | DATE SURVEY COMPLETED |
NAME OF PROVIDER OR SUPPLIER | STREET ADDRESS CITY STATE ZIP | ID PREFIX TAG |
SUMMARY STATEMENT OF DEFICIENCIES | PROVIDER'S PLAN OF CORRECTION | COMPLETION DATE |
Continued from page X | FORM CMS-2567 | Previous Versions Obsolete |
Event ID: | Facility ID: | If continuation sheet Page X of Y | signature lines | page numbers

CITATION BOUNDARY RULES:
- Citation STARTS: new F-tag + (SS= or CFR or regulatory title within 300 chars)
- Citation ENDS: next different F-tag begins, or document ends
- Same F-tag on continuation page = SAME citation, do NOT start new object
- F0000 = survey metadata, exclude from citations array entirely

REGULATION vs NARRATIVE:
- BEFORE "This REQUIREMENT is NOT MET as evidenced by:" → federal_requirement_text
- AFTER that phrase → deficiency_narrative_full

POST-PROCESSING — before returning, verify each citation:
1. No header/footer boilerplate in any field
2. No "Continued from page X" text in any field
3. No column labels in any field
4. No repeated facility metadata in any field
5. deficiency_narrative starts only after "This REQUIREMENT is NOT MET"
6. F0000 excluded

Return one top-level JSON object with survey_metadata and citations array.
Enable extraction_debug with excluded_text_samples showing what was discarded.

Schema:
{
  "survey_metadata": {"provider_name":"","provider_number":"","survey_completed_date":"","facility_address":""},
  "citations": [{
    "tag_number": "F####",
    "scope_severity": "",
    "scope_severity_raw": "",
    "regulatory_title": "",
    "cfr_citations": [],
    "federal_requirement_text": "",
    "deficiency_summary": "",
    "deficiency_narrative_full": "",
    "sample_scope_text": "",
    "harm_or_risk_statement": "",
    "observation_evidence": "",
    "interview_evidence": "",
    "record_review_evidence": "",
    "affected_residents": [{"resident_id":"","citation_specific_issue":"","dates_mentioned":[],"diagnoses_or_conditions":[],"related_staff":[]}],
    "staff_statements": [],
    "state_regulatory_references": [],
    "direct_quotes": [],
    "deficiency_category": "",
    "department_owner": "",
    "poc_inputs": {"core_problem_statement":"","what_failed":"","who_was_affected":"","why_this_matters":"","immediate_correction_candidates":[],"systemic_issue_candidates":[],"monitoring_candidates":[],"training_candidates":[],"documentation_gaps":[]}
  }],
  "extraction_debug": {"excluded_text_samples":[],"possible_noise_removed":[]}
}

Return ONLY valid JSON. Nothing before { or after }.`;

    // Build batch groups upfront
    const batches = [];
    for (let bi = 0; bi < tagBlocks.length; bi += BATCH_SIZE) {
      batches.push({ startIdx: bi, blocks: tagBlocks.slice(bi, bi + BATCH_SIZE) });
    }

    // Process one batch
    const processBatch = async ({ startIdx, blocks }) => {
      const batchNum = Math.floor(startIdx / BATCH_SIZE) + 1;
      job.currentChunk = Math.max(job.currentChunk || 0, batchNum);

      const batchPrompt = "Extract deficiency citation details for " + blocks.length + " CMS-2567 citation blocks.\n\n" +
        blocks.map(b => "=== TAG: " + b.tag + " ===\n" + b.text.slice(0, 2500)).join("\n\n") +
        "\n\nReturn a JSON array with exactly " + blocks.length + " objects in order. " +
        "Each object: {tag_number,scope_severity,scope_severity_raw,regulatory_title,cfr_citations,federal_requirement_text," +
        "deficiency_summary,deficiency_narrative_full,harm_or_risk_statement,observation_evidence,interview_evidence," +
        "record_review_evidence,affected_residents,staff_statements,state_regulatory_references,direct_quotes," +
        "deficiency_category,department_owner,poc_inputs}. Return ONLY the JSON array.";

      const EMPTY = (tag) => ({ tag, tag_number: tag, scope_severity:"", title:"", cfr_citation:"", deficiency_statement:"", observations:"", residents_affected:"", deficiency_narrative_full:"", harm_or_risk_statement:"", deficiency_category:"", department_owner:"", cfr_citations:[], affected_residents_detail:[], staff_statements:[], state_regulatory_references:[], direct_quotes:[], poc_inputs:{} });

      try {
        const command = new InvokeModelCommand({
          modelId: BEDROCK_MODEL_ID, contentType: "application/json", accept: "application/json",
          body: JSON.stringify({ anthropic_version: "bedrock-2023-05-31", max_tokens: 10000, system: systemPrompt, messages: [{ role: "user", content: batchPrompt }] }),
        });
        const resp = await client.send(command);
        const text = JSON.parse(new TextDecoder().decode(resp.body))?.content?.[0]?.text || "";
        const arrStart = text.indexOf("["), arrEnd = text.lastIndexOf("]");
        const objStart = text.indexOf("{"), objEnd = text.lastIndexOf("}");
        let batchResults = null;
        if (arrStart !== -1 && arrEnd !== -1) batchResults = JSON.parse(text.slice(arrStart, arrEnd + 1));
        else if (objStart !== -1 && objEnd !== -1) { const o = JSON.parse(text.slice(objStart, objEnd + 1)); batchResults = o.citations || null; }
        if (batchResults) {
          blocks.forEach((b, i) => {
            const d = batchResults[i] || {};
            allCitations[startIdx + i] = {
              tag: b.tag,
              tag_number: b.tag,   // canonical field — always from regex, never from AI
              scope_severity: d.scope_severity || "",
              title: d.regulatory_title || d.title || "",
              cfr_citation: Array.isArray(d.cfr_citations) ? d.cfr_citations.join(", ") : (d.cfr_citation || ""),
              deficiency_statement: d.deficiency_summary || d.deficiency_statement || "",
              observations: [d.observation_evidence, d.interview_evidence, d.record_review_evidence].filter(Boolean).join(" | ").slice(0, 500) || "",
              residents_affected: Array.isArray(d.affected_residents) ? d.affected_residents.map(r => r.resident_id || r).filter(Boolean).join(", ") : (d.residents_affected || ""),
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
            };
          });
          console.log("[Parse " + jobId + "] Batch " + batchNum + "/" + batches.length + " done");
        } else {
          blocks.forEach((b, i) => { allCitations[startIdx + i] = EMPTY(b.tag); });
          console.warn("[Parse " + jobId + "] Batch " + batchNum + " — no valid JSON");
        }
      } catch(e) {
        blocks.forEach((b, i) => { allCitations[startIdx + i] = EMPTY(b.tag); });
        console.warn("[Parse " + jobId + "] Batch " + batchNum + " failed:", e.message);
      }
    };

    // Run all batches in parallel with concurrency limit
    const batchQueue = [...batches];
    const workers = Array(Math.min(CONCURRENCY, Math.max(1, batchQueue.length))).fill(null).map(async () => {
      while (batchQueue.length > 0) { const b = batchQueue.shift(); if (b) await processBatch(b); }
    });
    await Promise.all([...workers, headerPromise]);

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
    console.log("[Parse " + jobId + "] LLM extracted: " + extractedCount + " of " + filteredTagPositions.length + " tags");

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

    job.status = "complete";
    job.result = {
      facility_name: facilityName2,
      survey_date: surveyDate2,
      survey_type: surveyType2,
      citations: deduped,
      validation_summary: {
        total: deduped.length,
        approved: approvedCount,
        needs_review: humanReviewCount,
        hard_fails: hardFailCount,
        candidate_count: filteredTagPositions.length
      },
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

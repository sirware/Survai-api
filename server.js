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


// ─── Supabase Auth Admin Routes ───────────────────────────────────────────────
// These use the Supabase service role key to manage Auth users server-side
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SECRET_KEY;

async function supabaseAdminFetch(path, method, body) {
  const response = await fetch(`${SUPABASE_URL}/auth/v1/admin${path}`, {
    method,
    headers: {
      "Content-Type": "application/json",
      "apikey": SUPABASE_SERVICE_KEY,
      "Authorization": `Bearer ${SUPABASE_SERVICE_KEY}`,
    },
    body: body ? JSON.stringify(body) : undefined,
  });
  return response.json();
}

// Create a new Supabase Auth user (called when adding a user in the app)
app.post("/api/auth/create-user", async (req, res) => {
  const { email, password, name } = req.body;
  if (!email || !password) return res.status(400).json({ error: "email and password required" });
  try {
    const data = await supabaseAdminFetch("/users", "POST", {
      email,
      password,
      email_confirm: true,
      user_metadata: { name },
    });
    if (data.error) {
      // User may already exist — try to update password instead
      console.warn("Create user error:", data.error);
      return res.status(200).json({ success: false, reason: data.error.message });
    }
    console.log(`Supabase Auth user created: ${email}`);
    return res.status(200).json({ success: true, id: data.id });
  } catch (err) {
    console.error("/api/auth/create-user error:", err.message);
    return res.status(500).json({ error: err.message });
  }
});

// Reset a user's password in Supabase Auth (called from admin password reset)
app.post("/api/auth/reset-password", async (req, res) => {
  const { email, password } = req.body;
  if (!email || !password) return res.status(400).json({ error: "email and password required" });
  try {
    // Find the user by email first
    const listData = await supabaseAdminFetch(`/users?email=${encodeURIComponent(email)}`, "GET");
    const user = listData.users?.[0];
    if (!user) {
      // User doesn't exist in Supabase Auth yet — create them
      const createData = await supabaseAdminFetch("/users", "POST", {
        email,
        password,
        email_confirm: true,
      });
      if (createData.error) return res.status(400).json({ error: createData.error.message });
      return res.status(200).json({ success: true, created: true });
    }
    // Update existing user's password
    const updateData = await supabaseAdminFetch(`/users/${user.id}`, "PUT", { password });
    if (updateData.error) return res.status(400).json({ error: updateData.error.message });
    console.log(`Supabase Auth password reset: ${email}`);
    return res.status(200).json({ success: true });
  } catch (err) {
    console.error("/api/auth/reset-password error:", err.message);
    return res.status(500).json({ error: err.message });
  }
});

// Migrate existing users to Supabase Auth (one-time call)
app.post("/api/auth/migrate-users", async (req, res) => {
  const { users } = req.body;
  if (!users?.length) return res.status(400).json({ error: "users array required" });
  const results = [];
  for (const u of users) {
    try {
      const data = await supabaseAdminFetch("/users", "POST", {
        email: u.email,
        password: u.tempPassword,
        email_confirm: true,
        user_metadata: { name: u.name },
      });
      results.push({ email: u.email, success: !data.error, error: data.error?.message });
    } catch(e) {
      results.push({ email: u.email, success: false, error: e.message });
    }
  }
  console.log(`Migration: ${results.filter(r => r.success).length}/${results.length} users created`);
  return res.status(200).json({ results });
});

app.listen(PORT, () => console.log(`SurvAI API running on port ${PORT}`));

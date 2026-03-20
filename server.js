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
function welcomeEmailHtml(name, email, tempPassword, role, facilityName) {
  return `
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Welcome to SurvAIHealth</title>
</head>
<body style="margin:0;padding:0;background:#f1f5f9;font-family:Arial,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#f1f5f9;padding:40px 20px;">
    <tr><td align="center">
      <table width="600" cellpadding="0" cellspacing="0" style="background:white;border-radius:12px;overflow:hidden;box-shadow:0 4px 24px rgba(0,0,0,0.08);">
        
        <!-- Header -->
        <tr><td style="background:linear-gradient(135deg,#0b3660,#0f4c81);padding:32px 40px;text-align:center;">
          <div style="font-size:28px;font-weight:800;color:white;letter-spacing:-0.02em;">SurvAI<span style="color:#38bdf8;">Health</span></div>
          <div style="color:#93c5fd;font-size:13px;margin-top:6px;text-transform:uppercase;letter-spacing:0.1em;">Quality Care, Intelligently Managed</div>
        </td></tr>

        <!-- Body -->
        <tr><td style="padding:40px;">
          <h2 style="color:#0f172a;font-size:22px;margin:0 0 8px;">Welcome to SurvAIHealth, ${name}! 👋</h2>
          <p style="color:#475569;font-size:15px;line-height:1.6;margin:0 0 24px;">Your account has been created. You can now log in and start managing Plans of Correction for your facility.</p>

          <!-- Credentials Box -->
          <table width="100%" cellpadding="0" cellspacing="0" style="background:#f0f9ff;border:1px solid #bae6fd;border-radius:10px;margin-bottom:28px;">
            <tr><td style="padding:24px;">
              <div style="font-size:13px;font-weight:700;color:#0369a1;text-transform:uppercase;letter-spacing:0.08em;margin-bottom:16px;">Your Login Credentials</div>
              <table width="100%" cellpadding="0" cellspacing="0">
                <tr>
                  <td style="padding:8px 0;color:#64748b;font-size:14px;width:130px;">Email:</td>
                  <td style="padding:8px 0;color:#0f172a;font-size:14px;font-weight:600;">${email}</td>
                </tr>
                <tr>
                  <td style="padding:8px 0;color:#64748b;font-size:14px;">Temp Password:</td>
                  <td style="padding:8px 0;">
                    <span style="background:#0f4c81;color:white;padding:6px 14px;border-radius:6px;font-size:15px;font-weight:700;font-family:monospace;letter-spacing:0.05em;">${tempPassword}</span>
                  </td>
                </tr>
                <tr>
                  <td style="padding:8px 0;color:#64748b;font-size:14px;">Role:</td>
                  <td style="padding:8px 0;color:#0f172a;font-size:14px;font-weight:600;">${role}</td>
                </tr>
                ${facilityName ? `<tr>
                  <td style="padding:8px 0;color:#64748b;font-size:14px;">Facility:</td>
                  <td style="padding:8px 0;color:#0f172a;font-size:14px;font-weight:600;">${facilityName}</td>
                </tr>` : ""}
              </table>
            </td></tr>
          </table>

          <!-- Warning -->
          <table width="100%" cellpadding="0" cellspacing="0" style="background:#fff7ed;border:1px solid #fed7aa;border-radius:8px;margin-bottom:28px;">
            <tr><td style="padding:16px 20px;">
              <div style="color:#92400e;font-size:14px;"><strong>⚠ Important:</strong> Please change your password after your first login. Go to your profile settings to update it.</div>
            </td></tr>
          </table>

          <!-- CTA Button -->
          <table width="100%" cellpadding="0" cellspacing="0">
            <tr><td align="center" style="padding:8px 0 28px;">
              <a href="https://survaihealth.com" style="background:linear-gradient(135deg,#0f4c81,#0891b2);color:white;text-decoration:none;padding:14px 36px;border-radius:8px;font-size:15px;font-weight:700;display:inline-block;">Sign In to SurvAIHealth →</a>
            </td></tr>
          </table>

          <p style="color:#94a3b8;font-size:13px;line-height:1.6;margin:0;">If you have any questions or need help getting started, contact your administrator or reply to this email.</p>
        </td></tr>

        <!-- Footer -->
        <tr><td style="background:#f8fafc;border-top:1px solid #e2e8f0;padding:20px 40px;text-align:center;">
          <p style="color:#94a3b8;font-size:12px;margin:0;">SurvAIHealth LLC · Quality Care, Intelligently Managed</p>
          <p style="color:#cbd5e1;font-size:11px;margin:4px 0 0;">This is an automated message. Please do not reply directly to this email.</p>
        </td></tr>

      </table>
    </td></tr>
  </table>
</body>
</html>`;
}

function deadlineReminderHtml(facilityName, tags, surveyDate, complianceDate, daysLeft, pocUrl) {
  const urgencyColor = daysLeft < 0 ? "#dc2626" : daysLeft <= 3 ? "#ea580c" : daysLeft <= 7 ? "#d97706" : "#0891b2";
  const urgencyBg = daysLeft < 0 ? "#fef2f2" : daysLeft <= 3 ? "#fff7ed" : daysLeft <= 7 ? "#fffbeb" : "#e0f2fe";
  const urgencyBorder = daysLeft < 0 ? "#fecaca" : daysLeft <= 3 ? "#fed7aa" : daysLeft <= 7 ? "#fde68a" : "#bae6fd";
  const urgencyLabel = daysLeft < 0 ? `OVERDUE BY ${Math.abs(daysLeft)} DAY${Math.abs(daysLeft) !== 1 ? "S" : ""}` : daysLeft === 0 ? "DUE TODAY" : `${daysLeft} DAY${daysLeft !== 1 ? "S" : ""} REMAINING`;
  const urgencyEmoji = daysLeft < 0 ? "🚨" : daysLeft <= 3 ? "🔴" : daysLeft <= 7 ? "🟡" : "📅";

  return `
<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"></head>
<body style="margin:0;padding:0;background:#f1f5f9;font-family:Arial,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#f1f5f9;padding:40px 20px;">
    <tr><td align="center">
      <table width="600" cellpadding="0" cellspacing="0" style="background:white;border-radius:12px;overflow:hidden;box-shadow:0 4px 24px rgba(0,0,0,0.08);">

        <!-- Header -->
        <tr><td style="background:linear-gradient(135deg,#0b3660,#0f4c81);padding:28px 40px;text-align:center;">
          <div style="font-size:26px;font-weight:800;color:white;">SurvAI<span style="color:#38bdf8;">Health</span></div>
          <div style="color:#93c5fd;font-size:12px;margin-top:4px;text-transform:uppercase;letter-spacing:0.1em;">Compliance Deadline Alert</div>
        </td></tr>

        <!-- Urgency Banner -->
        <tr><td style="background:${urgencyBg};border-bottom:2px solid ${urgencyBorder};padding:20px 40px;text-align:center;">
          <div style="font-size:32px;margin-bottom:8px;">${urgencyEmoji}</div>
          <div style="font-size:22px;font-weight:800;color:${urgencyColor};">${urgencyLabel}</div>
          <div style="font-size:14px;color:#64748b;margin-top:4px;">Plan of Correction Compliance Deadline</div>
        </td></tr>

        <!-- Details -->
        <tr><td style="padding:32px 40px;">
          <table width="100%" cellpadding="0" cellspacing="0" style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:10px;margin-bottom:24px;">
            <tr><td style="padding:20px 24px;">
              <div style="font-size:13px;font-weight:700;color:#64748b;text-transform:uppercase;letter-spacing:0.08em;margin-bottom:14px;">Survey Details</div>
              <table width="100%" cellpadding="0" cellspacing="0">
                <tr>
                  <td style="padding:6px 0;color:#64748b;font-size:14px;width:160px;">Facility:</td>
                  <td style="padding:6px 0;color:#0f172a;font-size:14px;font-weight:700;">${facilityName}</td>
                </tr>
                <tr>
                  <td style="padding:6px 0;color:#64748b;font-size:14px;">Tags Cited:</td>
                  <td style="padding:6px 0;color:#0f172a;font-size:14px;font-weight:600;">${tags.join(", ")}</td>
                </tr>
                <tr>
                  <td style="padding:6px 0;color:#64748b;font-size:14px;">Survey Date:</td>
                  <td style="padding:6px 0;color:#0f172a;font-size:14px;">${surveyDate}</td>
                </tr>
                <tr>
                  <td style="padding:6px 0;color:#64748b;font-size:14px;">Compliance Date:</td>
                  <td style="padding:6px 0;color:${urgencyColor};font-size:15px;font-weight:800;">${complianceDate}</td>
                </tr>
              </table>
            </td></tr>
          </table>

          <p style="color:#475569;font-size:14px;line-height:1.6;margin:0 0 24px;">
            ${daysLeft < 0 
              ? "This Plan of Correction is past its compliance deadline. Immediate action is required. Contact your state agency if you have not already submitted."
              : daysLeft === 0
              ? "Your Plan of Correction compliance deadline is today. Ensure all corrective actions are complete and documented."
              : `Your Plan of Correction compliance deadline is approaching. Please ensure all corrective actions are on track and properly documented before the deadline.`
            }
          </p>

          <!-- CTA -->
          <table width="100%" cellpadding="0" cellspacing="0">
            <tr><td align="center" style="padding:4px 0 24px;">
              <a href="https://survaihealth.com" style="background:linear-gradient(135deg,#0f4c81,#0891b2);color:white;text-decoration:none;padding:13px 32px;border-radius:8px;font-size:14px;font-weight:700;display:inline-block;">View Plan of Correction →</a>
            </td></tr>
          </table>

          <p style="color:#94a3b8;font-size:12px;line-height:1.6;margin:0;text-align:center;">This is an automated reminder from SurvAIHealth. You are receiving this because you are listed as a responsible party for this facility's compliance.</p>
        </td></tr>

        <!-- Footer -->
        <tr><td style="background:#f8fafc;border-top:1px solid #e2e8f0;padding:16px 40px;text-align:center;">
          <p style="color:#94a3b8;font-size:12px;margin:0;">SurvAIHealth LLC · Quality Care, Intelligently Managed</p>
        </td></tr>

      </table>
    </td></tr>
  </table>
</body>
</html>`;
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
    max_tokens: rest.max_tokens || 4096,
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

// ─── Send Welcome Email (new user created) ────────────────────────────────────
app.post("/api/email/welcome", async (req, res) => {
  const { name, email, tempPassword, role, facilityName } = req.body;

  if (!name || !email || !tempPassword) {
    return res.status(400).json({ error: "name, email, and tempPassword are required" });
  }

  const roleLabels = {
    admin: "System Administrator",
    regional: "Regional Director",
    facility_admin: "Facility Administrator",
    editor: "Editor",
    staff: "Staff Member",
    viewer: "Viewer",
  };

  const subject = `Welcome to SurvAIHealth — Your Account is Ready`;
  const html = welcomeEmailHtml(name, email, tempPassword, roleLabels[role] || role, facilityName);
  const text = `Welcome to SurvAIHealth, ${name}!\n\nYour account has been created.\n\nEmail: ${email}\nTemporary Password: ${tempPassword}\nRole: ${roleLabels[role] || role}${facilityName ? `\nFacility: ${facilityName}` : ""}\n\nSign in at: https://survaihealth.com\n\nPlease change your password after your first login.\n\nSurvAIHealth LLC`;

  const result = await sendEmail(email, subject, html, text);

  if (!result.success) {
    return res.status(500).json({ error: "Failed to send email", detail: result.reason });
  }

  console.log(`Welcome email sent to ${email}`);
  return res.status(200).json({ success: true, message: `Welcome email sent to ${email}` });
});

// ─── Send Deadline Reminder Email ─────────────────────────────────────────────
app.post("/api/email/deadline", async (req, res) => {
  const { recipients, facilityName, tags, surveyDate, complianceDate, daysLeft } = req.body;

  if (!recipients || !recipients.length || !facilityName || !complianceDate) {
    return res.status(400).json({ error: "recipients, facilityName, and complianceDate are required" });
  }

  const urgencyLabel = daysLeft < 0
    ? `OVERDUE — ${facilityName}`
    : daysLeft === 0
    ? `Due Today — ${facilityName} POC Deadline`
    : `${daysLeft} Day${daysLeft !== 1 ? "s" : ""} Remaining — ${facilityName} POC Deadline`;

  const subject = `⏰ ${urgencyLabel}`;
  const html = deadlineReminderHtml(facilityName, tags || [], surveyDate, complianceDate, daysLeft);
  const text = `SurvAIHealth Deadline Alert\n\nFacility: ${facilityName}\nTags: ${(tags || []).join(", ")}\nCompliance Date: ${complianceDate}\nStatus: ${daysLeft < 0 ? `OVERDUE by ${Math.abs(daysLeft)} days` : daysLeft === 0 ? "DUE TODAY" : `${daysLeft} days remaining`}\n\nLog in at https://survaihealth.com to review.\n\nSurvAIHealth LLC`;

  const results = await Promise.all(
    recipients.map(email => sendEmail(email, subject, html, text))
  );

  const failed = results.filter(r => !r.success);
  console.log(`Deadline reminder sent to ${recipients.length - failed.length}/${recipients.length} recipients for ${facilityName}`);

  return res.status(200).json({
    success: true,
    sent: recipients.length - failed.length,
    failed: failed.length,
  });
});

app.listen(PORT, () => console.log(`SurvAI API running on port ${PORT}`));

const express = require("express");
const cors = require("cors");

const app = express();
const PORT = process.env.PORT || 3001;

app.use(cors({ origin: "*" }));
app.use(express.json({ limit: "10mb" }));

// Health check
app.get("/", (req, res) => res.json({ status: "SurvAI API running" }));

// Main Claude proxy - no timeout limits
app.post("/api/claude", async (req, res) => {
  const apiKey = req.headers["x-api-key"] || req.body?.apiKey;
  if (!apiKey) return res.status(401).json({ error: { message: "API key required" } });

  const body = { ...req.body };
  delete body.apiKey;

  try {
    const response = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "anthropic-version": "2023-06-01",
        "x-api-key": apiKey,
      },
      body: JSON.stringify(body),
    });

    const data = await response.json();
    return res.status(response.status).json(data);
  } catch (err) {
    return res.status(500).json({ error: { message: err.message } });
  }
});

app.listen(PORT, () => console.log(`SurvAI API running on port ${PORT}`));

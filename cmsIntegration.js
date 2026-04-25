// ═══════════════════════════════════════════════════════════════════════════
// SurvAIHealth — CMS Public Data Catalog Integration
// ═══════════════════════════════════════════════════════════════════════════
// Fetches and caches CMS Provider Data Catalog data:
//   - Provider Information (4pq5-n9py) — facility ratings, ownership, beds
//   - Health Deficiencies (r5ix-sfxw) — F-tag citation history
//
// Architecture (Hybrid caching — Option C from our discussion):
//   - Weekly cron pulls fresh data into Supabase
//   - On-demand refresh available via /api/cms/refresh/:ccn
//   - App reads from cached Supabase tables for fast predictions
//
// IMPORTANT NOTES:
//   - All CMS public data — no authentication required
//   - Defensive parsing: tries multiple field name patterns since CMS APIs
//     occasionally rename columns between dataset versions
//   - Logs every fetch to cms_refresh_log for debugging
//   - Pagination handled — citations can be 100+ rows per facility
// ═══════════════════════════════════════════════════════════════════════════

// Note: We use the built-in global fetch (Node 18+). No node-fetch package required.
// Render runs Node 18+, so global fetch is always available.

// ─── Helper: fetch with timeout ─────────────────────────────────────────────
// Built-in fetch doesn't support a `timeout` option directly; we use AbortController.
async function fetchWithTimeout(url, timeoutMs = 30000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { signal: controller.signal });
  } finally {
    clearTimeout(timer);
  }
}

// ─── Dataset constants ──────────────────────────────────────────────────────
const PROVIDER_INFO_DATASET = "4pq5-n9py";
const HEALTH_DEFICIENCIES_DATASET = "r5ix-sfxw";
const CMS_API_BASE = "https://data.cms.gov/provider-data/api/1/datastore/query";

// ─── Helper: defensive field extraction ────────────────────────────────────
// CMS dataset field names occasionally change between versions. Try multiple
// known patterns and return the first match. Returns null if none found.
function getField(row, ...possibleNames) {
  for (const name of possibleNames) {
    if (row[name] !== undefined && row[name] !== null && row[name] !== "") {
      return row[name];
    }
  }
  return null;
}

function toInt(val) {
  if (val === null || val === undefined || val === "") return null;
  const n = parseInt(val, 10);
  return isNaN(n) ? null : n;
}

function toNumeric(val) {
  if (val === null || val === undefined || val === "") return null;
  const n = parseFloat(val);
  return isNaN(n) ? null : n;
}

function toBool(val) {
  if (val === null || val === undefined) return null;
  if (typeof val === "boolean") return val;
  const s = String(val).toLowerCase().trim();
  if (s === "y" || s === "yes" || s === "true" || s === "1") return true;
  if (s === "n" || s === "no" || s === "false" || s === "0") return false;
  return null;
}

function toDate(val) {
  if (!val) return null;
  // CMS dates come in various formats — try ISO first, then MM/DD/YYYY
  const d = new Date(val);
  if (!isNaN(d.getTime())) return d.toISOString().slice(0, 10);
  return null;
}

// Normalize F-tag — strip "F" and leading zeros, then re-format consistently
// "F0689" -> "F689", "F 689" -> "F689", "0689" -> "F689"
function normalizeTag(tag) {
  if (!tag) return null;
  const digits = String(tag).replace(/[^0-9]/g, "");
  if (!digits) return null;
  return "F" + parseInt(digits, 10);
}

// ─── Logging ────────────────────────────────────────────────────────────────
async function logRefresh(supabase, payload) {
  try {
    await supabase.from("cms_refresh_log").insert(payload);
  } catch (e) {
    console.error("[cmsIntegration] Failed to log refresh:", e.message);
  }
}

// ─── Provider Information API ──────────────────────────────────────────────
// Fetches one facility's provider info by CCN and upserts into cms_facility_data.
async function fetchProviderInfo(ccn) {
  const url = `${CMS_API_BASE}/${PROVIDER_INFO_DATASET}/0?conditions[0][property]=federal_provider_number&conditions[0][value]=${ccn}&conditions[0][operator]==&limit=1`;

  console.log(`[cmsIntegration] Fetching provider info for CCN ${ccn}`);
  const res = await fetchWithTimeout(url, 30000);
  if (!res.ok) {
    throw new Error(`CMS Provider API returned ${res.status}: ${res.statusText}`);
  }

  const data = await res.json();
  const results = data.results || data.data || data;

  if (!Array.isArray(results) || results.length === 0) {
    console.warn(`[cmsIntegration] No provider info found for CCN ${ccn}`);
    return null;
  }

  const row = results[0];

  // Defensive parsing — CMS uses different field names across dataset versions
  return {
    ccn,
    provider_name: getField(row, "provider_name", "facility_name", "name"),
    legal_business_name: getField(row, "legal_business_name", "legal_name"),
    address: getField(row, "provider_address", "address", "street_address"),
    city: getField(row, "provider_city", "city"),
    state: getField(row, "provider_state", "state"),
    zip: getField(row, "provider_zip_code", "zip_code", "zip"),
    phone: getField(row, "provider_phone_number", "phone_number", "phone"),
    county: getField(row, "provider_county_name", "county_name", "county"),

    overall_rating: toInt(getField(row, "overall_rating")),
    health_inspection_rating: toInt(getField(row, "health_inspection_rating", "rating_cycle_1_total_health_score")),
    staffing_rating: toInt(getField(row, "staffing_rating")),
    qm_rating: toInt(getField(row, "qm_rating", "quality_msr_rating")),

    number_of_certified_beds: toInt(getField(row, "number_of_certified_beds", "certified_beds")),
    ownership_type: getField(row, "ownership_type"),
    provider_type: getField(row, "provider_type", "provider_subtype"),
    in_hospital: toBool(getField(row, "with_a_resident_and_family_council", "located_in_hospital", "in_hospital")),
    in_ccrc: toBool(getField(row, "continuing_care_retirement_community", "in_ccrc")),
    legal_entity: getField(row, "legal_entity_type", "legal_entity"),
    resident_council: toBool(getField(row, "with_a_resident_council", "resident_council")),
    family_council: toBool(getField(row, "with_a_family_council", "family_council")),
    sprinkler_status: getField(row, "automatic_sprinkler_systems_in_all_required_areas", "sprinkler_status"),

    special_focus_status: getField(row, "special_focus_status"),
    abuse_icon: toBool(getField(row, "abuse_icon")),
    most_recent_health_inspection_date: toDate(getField(row, "date_first_approved_to_provide_medicare_and_medicaid_services", "most_recent_health_inspection_date")),
    total_amount_of_fines_in_dollars: toNumeric(getField(row, "total_amount_of_fines_in_dollars", "total_fines_in_dollars")),
    total_number_of_penalties: toInt(getField(row, "total_number_of_penalties")),
    total_number_of_health_deficiencies: toInt(getField(row, "total_number_of_health_deficiencies")),
    average_number_of_residents_per_day: toNumeric(getField(row, "average_number_of_residents_per_day")),

    chain_name: getField(row, "chain_name", "ownership_chain_name"),
    chain_id: getField(row, "chain_id"),

    cms_data_last_updated: toDate(getField(row, "processing_date", "data_as_of_date")),
    fetched_at: new Date().toISOString(),
    source_api: "cms_provider_data_api"
  };
}

// ─── Health Deficiencies API ───────────────────────────────────────────────
// Fetches all citations for a CCN. May return 100+ rows. Paginated.
async function fetchCitations(ccn) {
  const allCitations = [];
  let offset = 0;
  const pageSize = 500;
  let hasMore = true;

  console.log(`[cmsIntegration] Fetching citations for CCN ${ccn}`);

  while (hasMore && offset < 5000) { // hard cap at 5000 to prevent infinite loops
    const url = `${CMS_API_BASE}/${HEALTH_DEFICIENCIES_DATASET}/0?conditions[0][property]=federal_provider_number&conditions[0][value]=${ccn}&conditions[0][operator]==&limit=${pageSize}&offset=${offset}`;

    const res = await fetchWithTimeout(url, 30000);
    if (!res.ok) {
      throw new Error(`CMS Citations API returned ${res.status} at offset ${offset}`);
    }

    const data = await res.json();
    const results = data.results || data.data || data;

    if (!Array.isArray(results) || results.length === 0) {
      hasMore = false;
      break;
    }

    for (const row of results) {
      const tagRaw = getField(row, "deficiency_tag_number", "tag_number", "f_tag");
      const tag = normalizeTag(tagRaw);
      const surveyDate = toDate(getField(row, "survey_date", "inspection_date", "filedate"));

      if (!tag || !surveyDate) continue; // skip rows we can't identify

      allCitations.push({
        id: `${ccn}-${surveyDate}-${tag}-${offset + allCitations.length}`, // unique-ish composite
        ccn,
        survey_date: surveyDate,
        survey_type: getField(row, "survey_type", "deficiency_category"),
        deficiency_tag_number: tagRaw,
        tag_short: tag,
        tag_description: getField(row, "deficiency_tag_description", "tag_description", "deficiency_description"),
        scope_severity_code: getField(row, "scope_severity_code", "deficiency_scope_severity_code"),
        deficiency_corrected: getField(row, "deficiency_corrected"),
        correction_date: toDate(getField(row, "correction_date", "deficiency_corrected_date")),
        inspection_cycle: toInt(getField(row, "inspection_cycle", "cycle")),
        filedate: toDate(getField(row, "filedate", "file_date")),
        fetched_at: new Date().toISOString()
      });
    }

    if (results.length < pageSize) {
      hasMore = false;
    } else {
      offset += pageSize;
    }
  }

  console.log(`[cmsIntegration] Fetched ${allCitations.length} citations for CCN ${ccn}`);
  return allCitations;
}

// ─── Refresh single facility ───────────────────────────────────────────────
// Pulls both provider info + citations for one CCN, upserts to Supabase.
async function refreshFacility(supabase, ccn) {
  const startedAt = Date.now();
  let provider = null;
  let citations = [];
  let errors = [];

  // Step 1: Provider Info
  try {
    provider = await fetchProviderInfo(ccn);
    if (provider) {
      const { error } = await supabase
        .from("cms_facility_data")
        .upsert(provider, { onConflict: "ccn" });

      if (error) {
        errors.push(`provider_info_upsert: ${error.message}`);
      } else {
        console.log(`[cmsIntegration] Upserted provider info for ${ccn}: ${provider.provider_name}`);
      }
    }
  } catch (e) {
    errors.push(`provider_info_fetch: ${e.message}`);
    console.error(`[cmsIntegration] Provider info fetch failed for ${ccn}:`, e);
  }

  // Step 2: Citations (only if provider info succeeded — citations FK to facility)
  if (provider) {
    try {
      citations = await fetchCitations(ccn);

      if (citations.length > 0) {
        // Delete existing citations for this CCN, then insert fresh
        // (simpler than diffing; citations don't change after they're filed)
        const { error: delError } = await supabase
          .from("cms_facility_citations")
          .delete()
          .eq("ccn", ccn);

        if (delError) {
          errors.push(`citations_delete: ${delError.message}`);
        }

        // Batch insert in chunks of 500 to avoid Supabase row limits
        for (let i = 0; i < citations.length; i += 500) {
          const batch = citations.slice(i, i + 500);
          const { error: insError } = await supabase
            .from("cms_facility_citations")
            .insert(batch);

          if (insError) {
            errors.push(`citations_insert_batch_${i}: ${insError.message}`);
            break;
          }
        }
      }
    } catch (e) {
      errors.push(`citations_fetch: ${e.message}`);
      console.error(`[cmsIntegration] Citations fetch failed for ${ccn}:`, e);
    }
  }

  // Log the refresh attempt
  const durationMs = Date.now() - startedAt;
  await logRefresh(supabase, {
    endpoint: "refreshFacility",
    ccn,
    status: errors.length === 0 ? "success" : "partial_error",
    rows_fetched: citations.length,
    rows_inserted: citations.length,
    error_message: errors.length > 0 ? errors.join(" | ") : null,
    duration_ms: durationMs
  });

  return {
    ccn,
    provider_found: provider !== null,
    citations_count: citations.length,
    errors: errors.length > 0 ? errors : null,
    duration_ms: durationMs
  };
}

// ─── Refresh many facilities (cron-friendly) ───────────────────────────────
async function refreshManyFacilities(supabase, ccns, opts = {}) {
  const { delayBetweenMs = 1000, onProgress = null } = opts;
  const results = [];

  for (let i = 0; i < ccns.length; i++) {
    const ccn = ccns[i];
    try {
      const result = await refreshFacility(supabase, ccn);
      results.push(result);
      if (onProgress) onProgress(i + 1, ccns.length, result);
    } catch (e) {
      console.error(`[cmsIntegration] Catastrophic failure for ${ccn}:`, e);
      results.push({ ccn, error: e.message });
    }

    // Rate limit — be gentle to the CMS API
    if (i < ccns.length - 1) {
      await new Promise(r => setTimeout(r, delayBetweenMs));
    }
  }

  return results;
}

// ─── Compute state patterns aggregation ─────────────────────────────────────
// Aggregates citations by state + F-tag to identify state-level focus areas.
// Run monthly. Reads from cms_facility_citations + cms_facility_data, writes
// to cms_state_patterns.
async function computeStatePatterns(supabase) {
  const startedAt = Date.now();
  console.log("[cmsIntegration] Computing state patterns...");

  // Get all facilities by state for denominator
  const { data: facilities, error: facError } = await supabase
    .from("cms_facility_data")
    .select("ccn, state");

  if (facError) {
    console.error("[cmsIntegration] State patterns: failed to fetch facilities:", facError);
    return { error: facError.message };
  }

  const facilitiesByState = {};
  facilities.forEach(f => {
    if (!f.state) return;
    if (!facilitiesByState[f.state]) facilitiesByState[f.state] = new Set();
    facilitiesByState[f.state].add(f.ccn);
  });

  // Get all citations from last 12 months
  const oneYearAgo = new Date();
  oneYearAgo.setFullYear(oneYearAgo.getFullYear() - 1);
  const periodStart = oneYearAgo.toISOString().slice(0, 10);
  const periodEnd = new Date().toISOString().slice(0, 10);

  // Fetch citations with state info via join
  const { data: citations, error: citError } = await supabase
    .from("cms_facility_citations")
    .select("ccn, tag_short, survey_date, cms_facility_data!inner(state)")
    .gte("survey_date", periodStart);

  if (citError) {
    console.error("[cmsIntegration] State patterns: failed to fetch citations:", citError);
    return { error: citError.message };
  }

  // Aggregate
  const patterns = {}; // key: "state-tag"
  citations.forEach(c => {
    const state = c.cms_facility_data?.state;
    const tag = c.tag_short;
    if (!state || !tag) return;
    const key = `${state}-${tag}`;
    if (!patterns[key]) {
      patterns[key] = {
        state, tag,
        citation_count: 0,
        facility_set: new Set()
      };
    }
    patterns[key].citation_count++;
    patterns[key].facility_set.add(c.ccn);
  });

  // Convert to rows for upsert
  const rows = [];
  Object.values(patterns).forEach(p => {
    const totalInState = facilitiesByState[p.state]?.size || 0;
    rows.push({
      id: `${p.state}-${p.tag}-${periodStart}-${periodEnd}`,
      state: p.state,
      tag_short: p.tag,
      period_start: periodStart,
      period_end: periodEnd,
      citation_count: p.citation_count,
      facility_count: p.facility_set.size,
      total_facilities_in_state: totalInState,
      citation_rate: totalInState > 0 ? (p.facility_set.size / totalInState) : 0,
      computed_at: new Date().toISOString()
    });
  });

  // Compute national rank per tag (which states cite this tag most)
  const tagGroups = {};
  rows.forEach(r => {
    if (!tagGroups[r.tag_short]) tagGroups[r.tag_short] = [];
    tagGroups[r.tag_short].push(r);
  });
  Object.values(tagGroups).forEach(group => {
    group.sort((a, b) => b.citation_rate - a.citation_rate);
    group.forEach((r, i) => { r.national_rank = i + 1; });
  });

  // Clear old state patterns and insert fresh
  await supabase.from("cms_state_patterns").delete().neq("id", "");

  // Insert in batches
  for (let i = 0; i < rows.length; i += 500) {
    const batch = rows.slice(i, i + 500);
    const { error } = await supabase.from("cms_state_patterns").insert(batch);
    if (error) {
      console.error(`[cmsIntegration] State patterns insert error at batch ${i}:`, error);
    }
  }

  const durationMs = Date.now() - startedAt;
  await logRefresh(supabase, {
    endpoint: "computeStatePatterns",
    status: "success",
    rows_inserted: rows.length,
    duration_ms: durationMs
  });

  console.log(`[cmsIntegration] State patterns computed: ${rows.length} state-tag combinations in ${durationMs}ms`);
  return { rowCount: rows.length, durationMs };
}

// ─── Express route handlers ────────────────────────────────────────────────
// Mount these on your Express app:
//   const cms = require('./cmsIntegration');
//   app.get('/api/cms/facility/:ccn', cms.handleGetFacility(supabase));
//   app.post('/api/cms/refresh/:ccn', cms.handleRefreshFacility(supabase));
//   app.post('/api/cms/refresh-all', cms.handleRefreshAll(supabase));
//   app.post('/api/cms/state-patterns', cms.handleComputeStatePatterns(supabase));

function handleGetFacility(supabase) {
  return async (req, res) => {
    const ccn = req.params.ccn;
    if (!/^\d{6}$/.test(ccn)) {
      return res.status(400).json({ error: "Invalid CCN format (expected 6 digits)" });
    }

    try {
      const { data: facility, error: facError } = await supabase
        .from("cms_facility_data")
        .select("*")
        .eq("ccn", ccn)
        .single();

      if (facError || !facility) {
        return res.status(404).json({ error: "Facility not found in cache. Try POST /api/cms/refresh/:ccn first." });
      }

      const { data: citations, error: citError } = await supabase
        .from("cms_facility_citations")
        .select("*")
        .eq("ccn", ccn)
        .order("survey_date", { ascending: false });

      if (citError) {
        return res.status(500).json({ error: citError.message });
      }

      res.json({ facility, citations: citations || [] });
    } catch (e) {
      res.status(500).json({ error: e.message });
    }
  };
}

function handleRefreshFacility(supabase) {
  return async (req, res) => {
    const ccn = req.params.ccn;
    if (!/^\d{6}$/.test(ccn)) {
      return res.status(400).json({ error: "Invalid CCN format" });
    }

    try {
      const result = await refreshFacility(supabase, ccn);
      res.json(result);
    } catch (e) {
      res.status(500).json({ error: e.message });
    }
  };
}

function handleRefreshAll(supabase) {
  return async (req, res) => {
    try {
      // Get all CCNs from facilities table (i.e. our customer facilities + tracked prospects)
      const { data: facilities } = await supabase
        .from("facilities")
        .select("cms_ccn")
        .not("cms_ccn", "is", null);

      const ccns = [...new Set(facilities.map(f => f.cms_ccn))];

      // Run async — don't block the HTTP response on a long refresh
      res.json({ status: "started", facilities_to_refresh: ccns.length });

      refreshManyFacilities(supabase, ccns, {
        delayBetweenMs: 1500,
        onProgress: (i, total, result) => {
          console.log(`[cmsIntegration] Refresh progress: ${i}/${total} — ${result.ccn} ${result.errors ? "ERROR" : "OK"}`);
        }
      }).then(() => computeStatePatterns(supabase));
    } catch (e) {
      res.status(500).json({ error: e.message });
    }
  };
}

function handleComputeStatePatterns(supabase) {
  return async (req, res) => {
    try {
      const result = await computeStatePatterns(supabase);
      res.json(result);
    } catch (e) {
      res.status(500).json({ error: e.message });
    }
  };
}

// ─── Find facility by name (for the "Find on CMS" lookup tool) ─────────────
// Returns up to 20 matches based on partial name + state filter.
function handleFindFacility(supabase) {
  return async (req, res) => {
    const { name, state } = req.query;
    if (!name || name.length < 3) {
      return res.status(400).json({ error: "Name required (min 3 chars)" });
    }

    try {
      // Try CMS API first for live lookup
      const conditions = [
        `conditions[0][property]=provider_name&conditions[0][value]=${encodeURIComponent(name)}&conditions[0][operator]=contains`
      ];
      if (state) {
        conditions.push(`conditions[1][property]=provider_state&conditions[1][value]=${state}&conditions[1][operator]==`);
      }
      const url = `${CMS_API_BASE}/${PROVIDER_INFO_DATASET}/0?${conditions.join("&")}&limit=20`;

      const apiRes = await fetchWithTimeout(url, 15000);
      if (apiRes.ok) {
        const data = await apiRes.json();
        const results = data.results || data.data || data;
        const matches = (Array.isArray(results) ? results : []).map(row => ({
          ccn: getField(row, "federal_provider_number", "ccn"),
          provider_name: getField(row, "provider_name", "facility_name"),
          city: getField(row, "provider_city", "city"),
          state: getField(row, "provider_state", "state"),
          zip: getField(row, "provider_zip_code", "zip"),
          overall_rating: toInt(getField(row, "overall_rating")),
          number_of_certified_beds: toInt(getField(row, "number_of_certified_beds"))
        })).filter(m => m.ccn);

        return res.json({ matches, source: "live_api" });
      }

      // Fallback to cached data if API unreachable
      let q = supabase.from("cms_facility_data")
        .select("ccn, provider_name, city, state, zip, overall_rating, number_of_certified_beds")
        .ilike("provider_name", `%${name}%`)
        .limit(20);
      if (state) q = q.eq("state", state);

      const { data, error } = await q;
      if (error) return res.status(500).json({ error: error.message });
      res.json({ matches: data || [], source: "cache" });
    } catch (e) {
      res.status(500).json({ error: e.message });
    }
  };
}

// ─── Cron job entry point ──────────────────────────────────────────────────
// Schedule via Render cron (e.g., weekly Sundays at 2am):
//   const cms = require('./cmsIntegration');
//   cms.runWeeklyRefresh(supabase);
async function runWeeklyRefresh(supabase) {
  console.log("[cmsIntegration] Starting weekly refresh job");

  const { data: facilities, error } = await supabase
    .from("facilities")
    .select("cms_ccn")
    .not("cms_ccn", "is", null);

  if (error) {
    console.error("[cmsIntegration] Cron failed: cannot read facilities table:", error);
    return;
  }

  const ccns = [...new Set(facilities.map(f => f.cms_ccn).filter(Boolean))];
  console.log(`[cmsIntegration] Refreshing ${ccns.length} facilities`);

  await refreshManyFacilities(supabase, ccns, { delayBetweenMs: 2000 });
  await computeStatePatterns(supabase);

  console.log("[cmsIntegration] Weekly refresh complete");
}

module.exports = {
  // Direct fetch functions
  fetchProviderInfo,
  fetchCitations,
  refreshFacility,
  refreshManyFacilities,
  computeStatePatterns,
  runWeeklyRefresh,

  // Express handlers
  handleGetFacility,
  handleRefreshFacility,
  handleRefreshAll,
  handleComputeStatePatterns,
  handleFindFacility,

  // Helpers (exported for tests)
  normalizeTag,
  getField
};

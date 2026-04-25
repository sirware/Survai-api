// ═══════════════════════════════════════════════════════════════════════════
// SurvAIHealth — CMS Public Data Catalog Integration
// ═══════════════════════════════════════════════════════════════════════════
// Fetches and caches CMS Provider Data Catalog data:
//   - Provider Information      (4pq5-n9py)  — facility ratings, ownership, beds
//   - Health Deficiencies       (r5ix-sfxw)  — F-tag citation history
//
// Uses CMS DKAN datastore POST query endpoint (/datastore/query/{datasetId}/0)
// with a JSON request body. Per CMS OpenAPI spec, GET queries with array
// parameters (conditions, sorts) are "not reliably supported" — POST is the
// documented robust path.
//
// Architecture:
//   - On-demand refresh via /api/cms/refresh/:ccn
//   - Optional weekly cron pulls fresh data into Supabase
//   - App reads from cached Supabase tables for fast predictions
// ═══════════════════════════════════════════════════════════════════════════

// Built-in fetch (Node 18+). Render runs Node 18+, so global fetch is available.

// ─── Dataset constants ──────────────────────────────────────────────────────
const PROVIDER_INFO_DATASET = "4pq5-n9py";
const HEALTH_DEFICIENCIES_DATASET = "r5ix-sfxw";
const CMS_API_BASE = "https://data.cms.gov/provider-data/api/1/datastore/query";

// SQL endpoint distribution UUIDs (verified from CMS metastore April 2026)
const PROVIDER_INFO_UUID = "f87f2d80-0484-5229-8f0a-6398e5096de2";
const HEALTH_DEFICIENCIES_UUID = "49d544f4-2559-52ba-af3c-73a567be1c2b";
const CMS_SQL_BASE = "https://data.cms.gov/provider-data/api/1/datastore/sql";

// SQL query helper — uses CMS's SQL endpoint which is faster/lighter than POST query.
// Includes 120s timeout + 1 retry. show_db_columns=true returns machine field names.
async function sqlQuery(sqlText, timeoutMs = 120000, maxRetries = 3) {
  const url = `${CMS_SQL_BASE}?query=${encodeURIComponent(sqlText)}&show_db_columns`;
  let lastError;

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const res = await fetch(url, { signal: controller.signal });
      clearTimeout(timer);
      // Retry on 5xx server errors (CMS SQL endpoint occasionally returns 503 when busy)
      if (res.status >= 500 && res.status < 600 && attempt < maxRetries) {
        const backoffMs = 1500 * Math.pow(2, attempt); // 1.5s, 3s, 6s
        console.warn(`[cmsIntegration] SQL attempt ${attempt + 1} got ${res.status}, retrying in ${backoffMs}ms...`);
        await new Promise(r => setTimeout(r, backoffMs));
        continue;
      }
      return res;
    } catch (e) {
      clearTimeout(timer);
      lastError = e;
      if (attempt < maxRetries) {
        const backoffMs = 1500 * Math.pow(2, attempt);
        console.warn(`[cmsIntegration] SQL attempt ${attempt + 1} failed (${e.message}), retrying in ${backoffMs}ms...`);
        await new Promise(r => setTimeout(r, backoffMs));
      }
    }
  }
  if (lastError) throw lastError;
  // Fell through with non-OK status that's not a 5xx — return the last response so caller can read .ok
  throw new Error("SQL query failed after retries");
}

// ─── POST query helper with timeout + retry ────────────────────────────────
// Per CMS DKAN spec, query conditions go in a JSON body, not URL params.
// 120s timeout — CMS API can be slow on cold-cache hits.
// One automatic retry on AbortError or network failure.
async function postQuery(datasetId, queryBody, timeoutMs = 120000, maxRetries = 1) {
  const url = `${CMS_API_BASE}/${datasetId}/0`;
  let lastError;

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const res = await fetch(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Accept": "application/json",
        },
        body: JSON.stringify(queryBody),
        signal: controller.signal,
      });
      clearTimeout(timer);
      return res;
    } catch (e) {
      clearTimeout(timer);
      lastError = e;
      if (attempt < maxRetries) {
        console.warn(`[cmsIntegration] POST attempt ${attempt + 1} failed (${e.message}), retrying in 2s...`);
        await new Promise(r => setTimeout(r, 2000));
      }
    }
  }

  throw lastError;
}

// ─── Helper: defensive field extraction ────────────────────────────────────
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
  const d = new Date(val);
  if (!isNaN(d.getTime())) return d.toISOString().slice(0, 10);
  return null;
}

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
async function fetchProviderInfo(ccn) {
  console.log(`[cmsIntegration] Fetching provider info for CCN ${ccn}`);

  const sql = `[SELECT * FROM ${PROVIDER_INFO_UUID}][WHERE cms_certification_number_ccn = "${ccn}"][LIMIT 1]`;
  const res = await sqlQuery(sql);

  if (!res.ok) {
    const errBody = await res.text().catch(() => "");
    throw new Error(`CMS Provider API returned ${res.status}: ${res.statusText}. ${errBody.slice(0, 200)}`);
  }

  // SQL endpoint returns array directly (not wrapped in {results: []})
  const results = await res.json();

  if (!Array.isArray(results) || results.length === 0) {
    console.warn(`[cmsIntegration] No provider info found for CCN ${ccn}`);
    return null;
  }

  const row = results[0];

  return {
    ccn,
    provider_name: getField(row, "provider_name", "facility_name", "name"),
    legal_business_name: getField(row, "legal_business_name", "legal_name"),
    address: getField(row, "provider_address", "address", "street_address"),
    city: getField(row, "citytown", "provider_city", "city"),
    state: getField(row, "state", "provider_state"),
    zip: getField(row, "zip_code", "provider_zip_code", "zip"),
    phone: getField(row, "telephone_number", "provider_phone_number", "phone_number", "phone"),
    county: getField(row, "countyparish", "provider_county_name", "county_name", "county"),
    overall_rating: toInt(getField(row, "overall_rating")),
    health_inspection_rating: toInt(getField(row, "health_inspection_rating", "rating_cycle_1_total_health_score")),
    staffing_rating: toInt(getField(row, "staffing_rating")),
    qm_rating: toInt(getField(row, "qm_rating", "quality_msr_rating")),
    number_of_certified_beds: toInt(getField(row, "number_of_certified_beds", "certified_beds")),
    ownership_type: getField(row, "ownership_type"),
    provider_type: getField(row, "provider_type", "provider_subtype"),
    in_hospital: toBool(getField(row, "provider_resides_in_hospital", "located_in_hospital", "in_hospital")),
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
    source_api: "cms_provider_data_api",
    // ─── Staffing metrics (nested JSONB to avoid schema migration) ─────────
    // Used by Staffing Risk Forecast view
    staffing_metrics: {
      total_hprd:         toNumeric(getField(row, "reported_total_nurse_staffing_hours_per_resident_per_day")),
      rn_hprd:            toNumeric(getField(row, "reported_rn_staffing_hours_per_resident_per_day")),
      lpn_hprd:           toNumeric(getField(row, "reported_lpn_staffing_hours_per_resident_per_day")),
      cna_hprd:           toNumeric(getField(row, "reported_nurse_aide_staffing_hours_per_resident_per_day")),
      licensed_hprd:      toNumeric(getField(row, "reported_licensed_staffing_hours_per_resident_per_day")),
      weekend_total:      toNumeric(getField(row, "total_number_of_nurse_staff_hours_per_resident_per_day_on_t_4a14")),
      weekend_rn:         toNumeric(getField(row, "registered_nurse_hours_per_resident_per_day_on_the_weekend")),
      pt_hprd:            toNumeric(getField(row, "reported_physical_therapist_staffing_hours_per_resident_per_day")),
      nurse_turnover:     toNumeric(getField(row, "total_nursing_staff_turnover")),
      rn_turnover:        toNumeric(getField(row, "registered_nurse_turnover")),
      admin_turnover:     toInt(getField(row, "number_of_administrators_who_have_left_the_nursing_home")),
      casemix_index:      toNumeric(getField(row, "nursing_casemix_index")),
      casemix_index_ratio: toNumeric(getField(row, "nursing_casemix_index_ratio")),
      adjusted_total:     toNumeric(getField(row, "adjusted_total_nurse_staffing_hours_per_resident_per_day")),
      adjusted_rn:        toNumeric(getField(row, "adjusted_rn_staffing_hours_per_resident_per_day")),
      adjusted_weekend:   toNumeric(getField(row, "adjusted_weekend_total_nurse_staffing_hours_per_resident_per_day")),
    },
  };
}

// ─── Health Deficiencies API ───────────────────────────────────────────────
async function fetchCitations(ccn) {
  const allCitations = [];
  let offset = 0;
  const pageSize = 500;
  let hasMore = true;

  console.log(`[cmsIntegration] Fetching citations for CCN ${ccn}`);

  while (hasMore && offset < 5000) {
    const sql = `[SELECT * FROM ${HEALTH_DEFICIENCIES_UUID}][WHERE cms_certification_number_ccn = "${ccn}"][LIMIT ${pageSize} OFFSET ${offset}]`;
    const res = await sqlQuery(sql);

    if (!res.ok) {
      throw new Error(`CMS Citations API returned ${res.status} at offset ${offset}`);
    }

    // SQL endpoint returns array directly
    const results = await res.json();

    if (!Array.isArray(results) || results.length === 0) {
      hasMore = false;
      break;
    }

    for (const row of results) {
      const tagRaw = getField(row, "deficiency_tag_number", "tag_number", "f_tag");
      const tag = normalizeTag(tagRaw);
      const surveyDate = toDate(getField(row, "survey_date", "inspection_date", "filedate"));
      if (!tag || !surveyDate) continue;

      allCitations.push({
        id: `${ccn}-${surveyDate}-${tag}-${offset + allCitations.length}`,
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
        fetched_at: new Date().toISOString(),
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
async function refreshFacility(supabase, ccn) {
  const startedAt = Date.now();
  let provider = null;
  let citations = [];
  let errors = [];

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

  if (provider) {
    try {
      citations = await fetchCitations(ccn);
      if (citations.length > 0) {
        const { error: delError } = await supabase
          .from("cms_facility_citations")
          .delete()
          .eq("ccn", ccn);
        if (delError) errors.push(`citations_delete: ${delError.message}`);

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

  const durationMs = Date.now() - startedAt;
  await logRefresh(supabase, {
    endpoint: "refreshFacility",
    ccn,
    status: errors.length === 0 ? "success" : "partial_error",
    rows_fetched: citations.length,
    rows_inserted: citations.length,
    error_message: errors.length > 0 ? errors.join(" | ") : null,
    duration_ms: durationMs,
  });

  return {
    ccn,
    provider_found: provider !== null,
    citations_count: citations.length,
    errors: errors.length > 0 ? errors : null,
    duration_ms: durationMs,
  };
}

// ─── Refresh many facilities (cron-friendly) ───────────────────────────────
async function refreshManyFacilities(supabase, ccns, opts = {}) {
  const { delayBetweenMs = 1500, onProgress = null } = opts;
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
    if (i < ccns.length - 1) {
      await new Promise(r => setTimeout(r, delayBetweenMs));
    }
  }

  return results;
}

// ─── Compute state patterns aggregation ─────────────────────────────────────
async function computeStatePatterns(supabase) {
  const startedAt = Date.now();
  console.log("[cmsIntegration] Computing state patterns...");

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

  const oneYearAgo = new Date();
  oneYearAgo.setFullYear(oneYearAgo.getFullYear() - 1);
  const periodStart = oneYearAgo.toISOString().slice(0, 10);
  const periodEnd = new Date().toISOString().slice(0, 10);

  const { data: citations, error: citError } = await supabase
    .from("cms_facility_citations")
    .select("ccn, tag_short, survey_date, cms_facility_data!inner(state)")
    .gte("survey_date", periodStart);

  if (citError) {
    console.error("[cmsIntegration] State patterns: failed to fetch citations:", citError);
    return { error: citError.message };
  }

  const patterns = {};
  citations.forEach(c => {
    const state = c.cms_facility_data?.state;
    const tag = c.tag_short;
    if (!state || !tag) return;
    const key = `${state}-${tag}`;
    if (!patterns[key]) {
      patterns[key] = { state, tag, citation_count: 0, facility_set: new Set() };
    }
    patterns[key].citation_count++;
    patterns[key].facility_set.add(c.ccn);
  });

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
      computed_at: new Date().toISOString(),
    });
  });

  const tagGroups = {};
  rows.forEach(r => {
    if (!tagGroups[r.tag_short]) tagGroups[r.tag_short] = [];
    tagGroups[r.tag_short].push(r);
  });
  Object.values(tagGroups).forEach(group => {
    group.sort((a, b) => b.citation_rate - a.citation_rate);
    group.forEach((r, i) => { r.national_rank = i + 1; });
  });

  await supabase.from("cms_state_patterns").delete().neq("id", "");

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
    duration_ms: durationMs,
  });

  console.log(`[cmsIntegration] State patterns computed: ${rows.length} state-tag combinations in ${durationMs}ms`);
  return { rowCount: rows.length, durationMs };
}

// ─── Express route handlers ────────────────────────────────────────────────
function handleGetFacility(supabase) {
  return async (req, res) => {
    const ccn = req.params.ccn;
    if (!/^\d{6}$/.test(ccn)) {
      return res.status(400).json({ error: "Invalid CCN format (expected 6 digits)" });
    }
    try {
      const { data: facility, error: facError } = await supabase
        .from("cms_facility_data").select("*").eq("ccn", ccn).single();
      if (facError || !facility) {
        return res.status(404).json({ error: "Facility not found in cache. Try POST /api/cms/refresh/:ccn first." });
      }
      const { data: citations, error: citError } = await supabase
        .from("cms_facility_citations").select("*").eq("ccn", ccn).order("survey_date", { ascending: false });
      if (citError) return res.status(500).json({ error: citError.message });
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
      const { data: facilities } = await supabase
        .from("facilities").select("cms_ccn").not("cms_ccn", "is", null);
      const ccns = [...new Set(facilities.map(f => f.cms_ccn))];
      res.json({ status: "started", facilities_to_refresh: ccns.length });
      refreshManyFacilities(supabase, ccns, {
        delayBetweenMs: 1500,
        onProgress: (i, total, result) => {
          console.log(`[cmsIntegration] Refresh progress: ${i}/${total} — ${result.ccn} ${result.errors ? "ERROR" : "OK"}`);
        },
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

// ─── Find facility by name (for "Find on CMS" lookup tool) ─────────────────
function handleFindFacility(supabase) {
  return async (req, res) => {
    const { name, state, zip } = req.query;
    // Either a name (3+ chars) OR a 5-digit ZIP must be provided
    const hasName = name && String(name).length >= 3;
    const hasZip = zip && /^\d{5}$/.test(String(zip).trim());
    if (!hasName && !hasZip) {
      return res.status(400).json({ error: "Provide either name (min 3 chars) or zip (5 digits)" });
    }

    let liveResults = null;
    let liveError = null;

    // ─── Try CMS API live lookup via SQL endpoint ──────────────────────────
    try {
      const safeName = hasName ? String(name).toLowerCase() : null;
      const safeState = state ? String(state).replace(/"/g, "") : null;
      const safeZip = hasZip ? String(zip).trim() : null;

      // Build WHERE: ZIP wins if present (narrows to handful of facilities).
      // Otherwise state-only narrow + JS post-filter on name.
      let whereClause = "";
      if (safeZip) {
        whereClause = `[WHERE zip_code = "${safeZip}"]`;
      } else if (safeState) {
        whereClause = `[WHERE state = "${safeState}"]`;
      }
      const sql = `[SELECT cms_certification_number_ccn,provider_name,citytown,state,zip_code,overall_rating,number_of_certified_beds FROM ${PROVIDER_INFO_UUID}]${whereClause}[LIMIT 1000]`;

      const queryDesc = safeZip ? `ZIP ${safeZip}` : `"${name}"${state ? ` in ${state}` : ""}`;
      console.log(`[cmsIntegration] Find: SQL query for ${queryDesc}`);
      const apiRes = await sqlQuery(sql, 60000);

      if (apiRes.ok) {
        const allResults = await apiRes.json();
        // ZIP search returns the full set (already narrow). Name search post-filters by name.
        const filtered = safeZip
          ? (Array.isArray(allResults) ? allResults : [])
          : (Array.isArray(allResults) ? allResults : []).filter(row => {
              const n = (row.provider_name || "").toLowerCase();
              return n.includes(safeName);
            });
        const results = filtered.slice(0, 50);
        liveResults = (Array.isArray(results) ? results : []).map(row => ({
          ccn: getField(row, "cms_certification_number_ccn", "federal_provider_number", "ccn"),
          provider_name: getField(row, "provider_name", "facility_name"),
          city: getField(row, "citytown", "provider_city", "city"),
          state: getField(row, "state", "provider_state"),
          zip: getField(row, "zip_code", "provider_zip_code", "zip"),
          overall_rating: toInt(getField(row, "overall_rating")),
          number_of_certified_beds: toInt(getField(row, "number_of_certified_beds")),
        })).filter(m => m.ccn);

        if (liveResults.length > 0) {
          return res.json({ matches: liveResults, source: "live_api" });
        }
      } else {
        const errBody = await apiRes.text().catch(() => "");
        liveError = `CMS API returned ${apiRes.status}: ${errBody.slice(0, 120)}`;
      }
    } catch (e) {
      liveError = e.message;
      console.warn(`[cmsIntegration] Live CMS lookup failed: ${e.message}. Falling back to cache.`);
    }

    // ─── Fallback to Supabase cache ────────────────────────────────────────
    try {
      let q = supabase.from("cms_facility_data")
        .select("ccn, provider_name, city, state, zip, overall_rating, number_of_certified_beds")
        .ilike("provider_name", `%${name}%`).limit(20);
      if (state) q = q.eq("state", state);
      const { data, error } = await q;
      if (error) {
        return res.status(500).json({
          error: liveError ? `CMS live failed (${liveError}); cache also failed (${error.message})` : error.message,
        });
      }
      return res.json({ matches: data || [], source: "cache", live_error: liveError });
    } catch (e) {
      return res.status(500).json({
        error: liveError ? `CMS live failed (${liveError}); cache exception (${e.message})` : e.message,
      });
    }
  };
}

// ─── Cron entry point ──────────────────────────────────────────────────────
async function runWeeklyRefresh(supabase) {
  console.log("[cmsIntegration] Starting weekly refresh job");
  const { data: facilities, error } = await supabase
    .from("facilities").select("cms_ccn").not("cms_ccn", "is", null);
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

// ─── Snapshot handler — captures the PA brief for historical comparison ────
// Frontend POSTs the brief object after rendering. We dedupe by (ccn, date)
// so multiple views the same day count as one snapshot.
function handleSnapshotPrediction(supabase) {
  return async (req, res) => {
    try {
      const { cms_ccn, facility_id, focus_areas, watch_list, brief_full } = req.body || {};
      if (!cms_ccn) {
        return res.status(400).json({ error: "cms_ccn required" });
      }

      const today = new Date().toISOString().slice(0, 10); // YYYY-MM-DD

      // Already snapshotted today? Skip silently.
      const { data: existing } = await supabase
        .from("prediction_snapshots")
        .select("id")
        .eq("cms_ccn", cms_ccn)
        .eq("snapshot_date", today)
        .maybeSingle();

      if (existing) {
        return res.json({ status: "already_snapshotted", id: existing.id, snapshot_date: today });
      }

      const { data, error } = await supabase
        .from("prediction_snapshots")
        .insert([{
          cms_ccn,
          facility_id: facility_id || null,
          snapshot_date: today,
          focus_areas: focus_areas || [],
          watch_list: watch_list || [],
          brief_full: brief_full || null,
        }])
        .select()
        .single();

      if (error) {
        console.warn(`[cmsIntegration] Snapshot insert failed for ${cms_ccn}:`, error.message);
        return res.status(500).json({ error: error.message });
      }

      console.log(`[cmsIntegration] Snapshot captured for CCN ${cms_ccn} (${focus_areas?.length || 0} focus + ${watch_list?.length || 0} watch)`);
      res.json({ status: "snapshotted", id: data.id, snapshot_date: today });
    } catch (e) {
      console.warn("[cmsIntegration] Snapshot handler error:", e.message);
      res.status(500).json({ error: e.message });
    }
  };
}

// ─── State staffing medians — for "vs your state" comparison ───────────────
// Queries CMS SQL for all facilities in state, computes median for each
// staffing metric, caches in Supabase 24h. Used by Staffing Risk Forecast.
function handleStateStaffingMedians(supabase) {
  return async (req, res) => {
    try {
      const stateRaw = String(req.params.state || "").toUpperCase().trim();
      if (!/^[A-Z]{2}$/.test(stateRaw)) {
        return res.status(400).json({ error: "Invalid state code (need 2 letters)" });
      }

      // Check cache (< 24 hours old)
      const { data: cached } = await supabase
        .from("state_staffing_medians")
        .select("*")
        .eq("state", stateRaw)
        .maybeSingle();

      if (cached && cached.computed_at) {
        const ageMs = Date.now() - new Date(cached.computed_at).getTime();
        if (ageMs < 24 * 60 * 60 * 1000) {
          return res.json({ ...cached, source: "cache" });
        }
      }

      // Stale or missing — recompute from CMS
      console.log(`[cmsIntegration] Computing state staffing medians for ${stateRaw}`);
      const sql = `[SELECT reported_total_nurse_staffing_hours_per_resident_per_day,reported_rn_staffing_hours_per_resident_per_day,reported_lpn_staffing_hours_per_resident_per_day,reported_nurse_aide_staffing_hours_per_resident_per_day,registered_nurse_hours_per_resident_per_day_on_the_weekend,total_nursing_staff_turnover,registered_nurse_turnover FROM ${PROVIDER_INFO_UUID}][WHERE state = "${stateRaw}"][LIMIT 1500]`;
      const sqlRes = await sqlQuery(sql, 90000);
      if (!sqlRes.ok) {
        // If we have stale cache data, return it instead of a hard error so user still sees comparison
        if (cached) {
          console.warn(`[cmsIntegration] State medians fetch failed (${sqlRes.status}), serving stale cache for ${stateRaw}`);
          return res.json({ ...cached, source: "stale_cache", warning: `Live refresh failed (CMS returned ${sqlRes.status}), serving cached data from ${cached.computed_at}` });
        }
        return res.status(502).json({ error: `CMS SQL returned ${sqlRes.status}. Try again in a moment — CMS API is occasionally busy.` });
      }
      const rows = await sqlRes.json();
      if (!Array.isArray(rows) || rows.length === 0) {
        return res.status(404).json({ error: `No facilities found in state ${stateRaw}` });
      }

      // Median helper — strip non-numeric/zero values, sort, pick middle
      const median = (key) => {
        const vals = rows
          .map(r => parseFloat(r[key]))
          .filter(v => !isNaN(v) && v > 0)
          .sort((a, b) => a - b);
        if (vals.length === 0) return null;
        const mid = Math.floor(vals.length / 2);
        return vals.length % 2 ? vals[mid] : (vals[mid - 1] + vals[mid]) / 2;
      };

      const computed = {
        state: stateRaw,
        total_hprd: median("reported_total_nurse_staffing_hours_per_resident_per_day"),
        rn_hprd: median("reported_rn_staffing_hours_per_resident_per_day"),
        lpn_hprd: median("reported_lpn_staffing_hours_per_resident_per_day"),
        cna_hprd: median("reported_nurse_aide_staffing_hours_per_resident_per_day"),
        weekend_rn: median("registered_nurse_hours_per_resident_per_day_on_the_weekend"),
        nurse_turnover: median("total_nursing_staff_turnover"),
        rn_turnover: median("registered_nurse_turnover"),
        facility_count: rows.length,
        computed_at: new Date().toISOString(),
      };

      // Round for cleanliness
      Object.keys(computed).forEach(k => {
        if (typeof computed[k] === "number" && k !== "facility_count") {
          computed[k] = Math.round(computed[k] * 100) / 100;
        }
      });

      // Upsert to cache
      await supabase.from("state_staffing_medians").upsert([computed], { onConflict: "state" });

      console.log(`[cmsIntegration] Cached medians for ${stateRaw}: ${rows.length} facilities`);
      res.json({ ...computed, source: "live" });
    } catch (e) {
      console.warn("[cmsIntegration] State medians error:", e.message);
      res.status(500).json({ error: e.message });
    }
  };
}

module.exports = {
  fetchProviderInfo,
  fetchCitations,
  refreshFacility,
  refreshManyFacilities,
  computeStatePatterns,
  runWeeklyRefresh,
  handleGetFacility,
  handleRefreshFacility,
  handleRefreshAll,
  handleComputeStatePatterns,
  handleFindFacility,
  handleSnapshotPrediction,
  handleStateStaffingMedians,
  normalizeTag,
  getField,
};

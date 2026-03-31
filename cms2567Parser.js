// cms2567Parser.js — verbatim-first, merge-then-extract pipeline
'use strict';

const HEADER_NOISE_PATTERNS = [
  /^PRINTED:\s*.*/gim,
  /^DEPARTMENT OF HEALTH AND HUMAN SERVICES\s*$/gim,
  /^CENTERS FOR MEDICARE & MEDICAID SERVICES\s*$/gim,
  /^FORM APPROVED\s*$/gim,
  /^OMB NO\..*$/gim,
  /^STATEMENT OF DEFICIENCIES\s*$/gim,
  /^AND PLAN OF CORRECTIONS?\s*$/gim,
  /^FORM CMS-2567.*$/gim,
  /^If continuation sheet Page \d+ of \d+\s*$/gim,
];

const BLOCK_NOISE_PATTERNS = [
  /^PROVIDER\/SUPPLIER\/CLIA.*$/gim,
  /^NAME OF PROVIDER OR SUPPLIER.*$/gim,
  /^STREET ADDRESS,?\s*CITY,?\s*STATE,?\s*ZIP.*$/gim,
  /^SUMMARY STATEMENT OF DEFICIENCIES.*$/gim,
  /^PROVIDER.?S PLAN OF CORRECTION.*$/gim,
  /^ID\s*PREFIX\s*TAG.*$/gim,
  /^\(X[1-5]\).*$/gim,
  /^COMPLETION DATE.*$/gim,
  /^Event ID:.*$/gim,
  /^Facility ID:.*$/gim,
  /^LABORATORY DIRECTOR.*$/gim,
  /^TITLE\s+DATE\s+TIME.*$/gim,
];

function normalizeWs(text) {
  return (text || '').replace(/\r/g, '\n').replace(/[ \t]+/g, ' ').replace(/\n{3,}/g, '\n\n').trim();
}
function unique(arr) { return [...new Set((arr||[]).filter(Boolean))]; }
function safeMatch(t,re){ const m=t.match(re); return m?m[0].trim():''; }
function safeMatchAll(t,re){ const m=t.match(re); return m?unique(m.map(x=>x.trim())):[]; }

function cleanForSegmentation(raw) {
  let t = raw || '';
  for (const p of HEADER_NOISE_PATTERNS) t = t.replace(p, '');
  return normalizeWs(t);
}

function cleanBlock(block) {
  let t = block || '';
  for (const p of BLOCK_NOISE_PATTERNS) t = t.replace(p, '');
  t = t.replace(/Continued from page\s+\d+/gi, '');
  return normalizeWs(t);
}

// MERGE FIRST → THEN EXTRACT
// Split on every F-tag occurrence, accumulate same-tag blocks
function segmentCitationBlocks(rawText) {
  const cleaned = cleanForSegmentation(rawText);
  // Split keeping the delimiter
  const parts = cleaned.split(/(?=(?:^|\n)[FKE]\d{4}(?:\s|$))/m);
  const tagMap = new Map();
  const tagOrder = [];
  const TAG_RE = /^([FKE]\d{4})/;

  for (const part of parts) {
    const m = part.match(TAG_RE);
    if (!m) continue;
    const tag = m[1].toUpperCase();
    if (tag === 'F0000' || tag === 'K0000' || tag === 'E0000') continue;
    if (!tagMap.has(tag)) {
      tagMap.set(tag, part);
      tagOrder.push(tag);
    } else {
      // Same tag = continuation — APPEND (merge)
      tagMap.set(tag, tagMap.get(tag) + '\n' + part);
    }
  }

  return tagOrder.map(tag => {
    const raw = tagMap.get(tag);
    const ssM = raw.match(/SS\s*=\s*([A-L])/i);
    return {
      tag_number: tag,
      scope_severity_hint: ssM ? ssM[1].toUpperCase() : '',
      raw_block: raw,
      clean_block: cleanBlock(raw),
    };
  }).filter(b => b.raw_block.trim());
}

function buildFallbackCitation(rawBlock, cleanedBlock, tag, scopeSeverity) {
  const full = cleanedBlock || rawBlock || '';
  const ss = scopeSeverity || (full.match(/SS\s*=\s*([A-L])/i)||[])[1]?.toUpperCase() || '';
  const cfr = unique(safeMatchAll(full, /483\.\d+(?:\([a-z0-9]+\))*(?:-\([a-z0-9]+\))?/gi));

  // Regulatory title
  const escapedTag = tag.replace(/[.*+?^${}()|[\]\\]/g,'\\$&');
  const titleM = full.match(new RegExp(escapedTag+'[\\s\\S]{0,500}?SS\\s*=\\s*[A-L]?\\s*([\\s\\S]{0,300}?)\\s*CFR\\(s\\):','i'))
    || full.match(new RegExp(escapedTag+'[\\s\\S]{0,300}?\\n([^\\n]{5,120})\\n','i'));
  const regTitle = titleM ? normalizeWs(titleM[1]).replace(/^[FKE]\d{4}\s*/i,'').replace(/SS\s*=\s*[A-L]/i,'').trim().slice(0,120) : '';

  // Federal requirement vs narrative split
  const notMetIdx = full.search(/This REQUIREMENT is NOT MET as evidenced by:/i);
  const fedReq = notMetIdx > 0 ? normalizeWs(full.slice(0, notMetIdx)) : '';
  const narrativeRaw = notMetIdx > -1 ? full.slice(notMetIdx) : full.slice(Math.floor(full.length*0.3));
  const narrative = normalizeWs(narrativeRaw);

  // Deficiency statement — "The facility failed to" or first sentence
  const failedM = narrative.match(/The facility failed to[\s\S]*?\./i);
  const firstM = narrative.replace(/This REQUIREMENT is NOT MET as evidenced by:/i,'').trim().match(/^(.{20,400}?\.)/);
  const stmt = (failedM?failedM[0]:'') || (firstM?firstM[1]:'') || narrative.slice(0,300);

  // Evidence buckets
  const lines = narrative.split(/\n+/).map(s=>s.trim()).filter(Boolean);
  const obs=[], intv=[], rec=[];
  for (const l of lines) {
    if (/observation/i.test(l)) obs.push(l);
    if (/interview/i.test(l)) intv.push(l);
    if (/review of|record review|EHR|MAR|MDS|care plan/i.test(l)) rec.push(l);
  }

  const resIds = unique(safeMatchAll(full, /Resident\s+\d+/gi));
  const residents = resIds.map(rid => ({
    resident_id: rid, resident_description: '',
    diagnoses_or_conditions: [], decision_making_or_communication_status: '',
    citation_specific_issue: '',
    dates_mentioned: unique(safeMatchAll(full, /\b\d{2}\/\d{2}\/\d{4}\b/g)),
    related_staff: unique(safeMatchAll(full, /Staff\s+[A-Z]\b/g)),
    evidence: { record_review:'', interviews:'', observations:'' },
  }));

  return {
    tag, tag_number: tag,
    full_deficiency_text: full, // verbatim legal record
    scope_severity: ss, scope_severity_raw: ss ? `SS = ${ss}` : '',
    title: regTitle, regulatory_title: regTitle,
    cfr_citations: cfr, cfr_citation: cfr.join(', '),
    federal_requirement_text: fedReq,
    deficiency_narrative_full: narrative,
    deficiency_statement: stmt, deficiency_summary: stmt,
    sample_scope_text: safeMatch(narrative, /for \d+ of \d+ sampled residents[\s\S]*?(?:\)|\.)(?!\S)/i) || safeMatch(narrative, /for \d+ of \d+ sampled residents/i),
    harm_or_risk_statement: safeMatch(narrative, /This failure placed residents at risk of[\s\S]*?\./i),
    findings_full: safeMatch(narrative, /Findings included[\s\S]*/i),
    observation_evidence: obs.join(' '),
    interview_evidence: intv.join(' '),
    record_review_evidence: rec.join(' '),
    observations: [obs.join(' '), intv.join(' '), rec.join(' ')].filter(Boolean).join(' | ').slice(0,500),
    affected_residents: residents, affected_residents_detail: residents,
    residents_affected: resIds.join(', '),
    staff_statements: unique(safeMatchAll(full, /Staff\s+[A-Z][\s\S]*?(?:stated|reported|said)[\s\S]*?\./gi)),
    administrator_statements: unique(safeMatchAll(full, /Administrator[\s\S]*?(?:stated|reported|said)[\s\S]*?\./gi)),
    facility_expectation_statements: unique(safeMatchAll(full, /expectation[\s\S]*?\./gi)),
    policy_or_guideline_references: [],
    state_regulatory_references: unique(safeMatchAll(full, /\b(?:WAC|NMAC|OAR|TAC|FAC|IAC|AAC|Cal\. Code Regs\.)[\s\S]*?(?=\n|$)/gi)),
    direct_quotes: unique(safeMatchAll(full, /"[^"]+"|"[^"]+"/g)),
    deficiency_category: '', department_owner: '', poc_inputs: {},
    page_start: null, page_end: null,
    extraction_status: 'fallback', extraction_error: '',
    raw_block: cleanedBlock,
  };
}

// Hard fail ONLY: no tag, or block is clearly empty/header-only
function isMinimallyUsable(c) {
  if (!c || !c.tag_number || c.tag_number === 'F0000') return false;
  return (c.full_deficiency_text||c.deficiency_narrative_full||'').trim().length >= 20;
}

async function parseCMS2567(rawText, options = {}) {
  const { aiExtractor = null, concurrency = 4 } = options;
  const blocks = segmentCitationBlocks(rawText);

  // Extract F0000 Initial Comments as survey metadata (not a citation)
  let initialComments = '';
  try {
    const cleanedForF0000 = cleanForSegmentation(rawText);
    const f0000start = cleanedForF0000.search(/F0+\s+INITIAL COMMENTS/i);
    if (f0000start !== -1) {
      const afterF0000 = cleanedForF0000.slice(f0000start);
      const nextTagIdx = afterF0000.search(/\n[FKE]\d{3,4}\s/);
      const f0000end = nextTagIdx !== -1 ? f0000start + nextTagIdx : f0000start + 2000;
      initialComments = cleanedForF0000.slice(f0000start, f0000end)
        .replace(/F0+\s+INITIAL COMMENTS\s*/i, '')
        .trim();
    }
  } catch(e) {}

  if (!blocks.length) {
    return { document_type:'CMS-2567', citations:[], initial_comments: initialComments, stats:{ candidate_count:0, usable_count:0, ai_count:0, fallback_count:0 } };
  }

  const fallbacks = blocks.map(b => buildFallbackCitation(b.raw_block, b.clean_block, b.tag_number, b.scope_severity_hint));

  if (!aiExtractor) {
    const all = fallbacks.map(c => ({ ...c, requires_human_review: !isMinimallyUsable(c) }));
    return {
      document_type: 'CMS-2567', citations: all,
      initial_comments: initialComments,
      stats: { candidate_count: blocks.length, usable_count: all.filter(isMinimallyUsable).length, ai_count: 0, fallback_count: all.length },
    };
  }

  const enriched = new Array(fallbacks.length);
  let i = 0;
  async function worker() {
    while (i < fallbacks.length) {
      const idx = i++;
      const base = fallbacks[idx];
      try {
        const aiResult = await aiExtractor({ tag_number: base.tag_number, raw_block: base.raw_block, full_deficiency_text: base.full_deficiency_text, fallback: base });
        if (aiResult && typeof aiResult === 'object') {
          enriched[idx] = {
            ...base, ...aiResult,
            // NEVER overwrite verbatim legal fields
            tag: base.tag, tag_number: base.tag_number,
            full_deficiency_text: base.full_deficiency_text,
            deficiency_narrative_full: base.deficiency_narrative_full || aiResult.deficiency_narrative_full,
            raw_block: base.raw_block,
            extraction_status: 'ai', extraction_error: '',
            requires_human_review: !isMinimallyUsable(base),
          };
          // Never let AI blank out a fallback field
          for (const f of ['scope_severity','title','regulatory_title','cfr_citations','cfr_citation','deficiency_statement','deficiency_summary','federal_requirement_text','harm_or_risk_statement']) {
            if (!enriched[idx][f] && base[f]) enriched[idx][f] = base[f];
          }
        } else {
          enriched[idx] = { ...base, extraction_status:'fallback', extraction_error:'empty AI result' };
        }
      } catch(err) {
        enriched[idx] = { ...base, extraction_status:'fallback', extraction_error: err?.message||'AI failed' };
      }
    }
  }
  await Promise.all(Array.from({ length: Math.min(Math.max(concurrency,1), fallbacks.length) }, ()=>worker()));
  const result = enriched.map((c,i) => c||fallbacks[i]);
  return {
    document_type: 'CMS-2567', citations: result,
    initial_comments: initialComments,
    stats: {
      candidate_count: blocks.length,
      usable_count: result.filter(isMinimallyUsable).length,
      ai_count: result.filter(c=>c.extraction_status==='ai').length,
      fallback_count: result.filter(c=>c.extraction_status==='fallback').length,
    },
  };
}

module.exports = { cleanForSegmentation, cleanBlock, segmentCitationBlocks, buildFallbackCitation, isMinimallyUsable, parseCMS2567 };

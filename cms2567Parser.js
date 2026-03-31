// cms2567Parser.js
'use strict';

const TAG_START_RE = /(^|\n)(F\d{4})\s*(?:F\d{4})?\s*(?:\n|\s)*(?:SS\s*=\s*([A-L]))?/gi;

const HEADER_NOISE_PATTERNS = [
  /^PRINTED:\s*.*/gim,
  /^DEPARTMENT OF HEALTH AND HUMAN SERVICES\s*$/gim,
  /^CENTERS FOR MEDICARE & MEDICAID SERVICES\s*$/gim,
  /^FORM APPROVED\s*$/gim,
  /^OMB NO\..*$/gim,
  /^STATEMENT OF DEFICIENCIES\s*$/gim,
  /^AND PLAN OF CORRECTIONS\s*$/gim,
  /^FORM CMS-2567.*$/gim,
  /^If continuation sheet Page \d+ of \d+\s*$/gim,
];

const LIGHT_NOISE_PATTERNS = [
  /^PROVIDER\/SUPPLIER\/CLIA.*$/gim,
  /^NAME OF PROVIDER OR SUPPLIER.*$/gim,
  /^STREET ADDRESS, CITY, STATE, ZIP CODE.*$/gim,
  /^SUMMARY STATEMENT OF DEFICIENCIES.*$/gim,
  /^PROVIDER'S PLAN OF CORRECTION.*$/gim,
  /^ID\s*PREFIX\s*TAG.*$/gim,
  /^Event ID:.*$/gim,
  /^Facility ID:.*$/gim,
];

const CONTINUATION_RE = /^Continued from page \d+\s*$/gim;
const REGULATORY_TRIGGER_RE = /This REQUIREMENT is NOT MET as evidenced by:/i;
const DEFICIENT_PRACTICE_RE = /The facility failed to[\s\S]*?\./i;
const BASED_ON_RE = /Based on[\s\S]*?(?=The facility failed to|This failure placed residents at risk of|Findings included|$)/i;
const SAMPLE_SCOPE_RE = /for \d+ of \d+ sampled residents[\s\S]*?(?:\)|\.)(?!\S)/i;
const SAMPLE_SCOPE_ALT_RE = /for \d+ of \d+ sampled residents/i;
const HALL_SCOPE_RE = /for \d+ of \d+ halls?[\s\S]*?(?:\)|\.)(?!\S)/i;
const RISK_RE = /This failure placed residents at risk of[\s\S]*?\./i;
const FINDINGS_RE = /Findings included[\s\S]*/i;
const CFR_RE = /483\.\d+(?:\([a-z0-9]+\))*(?:-\([a-z0-9]+\))?/gi;
const RESIDENT_RE = /Resident\s+\d+/gi;
const STATE_REF_RE = /\b(?:WAC|NMAC|OAR|TAC|FAC|IAC|AAC|R\d+|Cal\. Code Regs\.)[\s\S]*?(?=\n|$)/gi;
const QUOTE_RE = /"[^"]+"|"[^"]+"/g;

function normalizeWhitespace(text) {
  return (text || '').replace(/\r/g, '\n').replace(/[ \t]+/g, ' ').replace(/\n{3,}/g, '\n\n').trim();
}

function unique(arr) {
  return [...new Set((arr || []).filter(Boolean))];
}

function safeMatch(text, regex) {
  const match = text.match(regex);
  return match ? match[0].trim() : '';
}

function safeMatchAll(text, regex) {
  const matches = text.match(regex);
  return matches ? unique(matches.map(x => x.trim())) : [];
}

function estimatePageRange() {
  return { page_start: null, page_end: null };
}

function cleanForSegmentation(rawText) {
  let text = rawText || '';
  for (const pattern of HEADER_NOISE_PATTERNS) {
    text = text.replace(pattern, '');
  }
  return normalizeWhitespace(text);
}

function cleanBlockAfterSegmentation(block) {
  let text = block || '';
  for (const pattern of LIGHT_NOISE_PATTERNS) {
    text = text.replace(pattern, '');
  }
  text = text.replace(CONTINUATION_RE, '');
  return normalizeWhitespace(text);
}

function extractScopeSeverity(rawBlock, existing = '') {
  if (existing) return existing.trim().toUpperCase();
  const m = rawBlock.match(/SS\s*=\s*([A-L])/i);
  return m ? m[1].toUpperCase() : '';
}

function extractRegulatoryTitle(rawBlock, tag) {
  if (!rawBlock) return '';
  const escapedTag = tag.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const re = new RegExp(`${escapedTag}[\\s\\S]{0,1000}?SS\\s*=\\s*[A-L]?\\s*([\\s\\S]{0,300}?)\\s*CFR\\(s\\):`, 'i');
  const m = rawBlock.match(re);
  if (m && m[1]) return normalizeWhitespace(m[1]);
  const backup = rawBlock.match(new RegExp(`${escapedTag}[\\s\\S]{0,500}?\\n([\\s\\S]{1,200}?)\\n\\s*CFR\\(s\\):`, 'i'));
  if (backup && backup[1]) return normalizeWhitespace(backup[1]);
  return '';
}

function extractFederalRequirementText(rawBlock) {
  if (!rawBlock) return '';
  const triggerIdx = rawBlock.search(REGULATORY_TRIGGER_RE);
  if (triggerIdx === -1) return '';
  const before = rawBlock.slice(0, triggerIdx);
  const cfrIdx = before.search(/CFR\(s\):/i);
  if (cfrIdx !== -1) return normalizeWhitespace(before.slice(cfrIdx));
  return normalizeWhitespace(before);
}

function extractNarrative(rawBlock) {
  if (!rawBlock) return '';
  const m = rawBlock.match(/This REQUIREMENT is NOT MET as evidenced by:[\s\S]*/i);
  return m ? normalizeWhitespace(m[0]) : '';
}

function extractDeficiencySummary(narrative) {
  if (!narrative) return '';
  const direct = safeMatch(narrative, DEFICIENT_PRACTICE_RE);
  if (direct) return direct;
  const stripped = narrative.replace(/This REQUIREMENT is NOT MET as evidenced by:/i, '').trim();
  const firstSentence = stripped.match(/^(.{20,400}?\.)/);
  return firstSentence ? normalizeWhitespace(firstSentence[1]) : '';
}

function extractEvidenceLead(narrative) {
  return safeMatch(narrative, BASED_ON_RE);
}

function extractSampleScope(narrative) {
  return (
    safeMatch(narrative, SAMPLE_SCOPE_RE) ||
    safeMatch(narrative, SAMPLE_SCOPE_ALT_RE) ||
    safeMatch(narrative, HALL_SCOPE_RE) ||
    ''
  );
}

function extractRiskStatement(narrative) {
  return safeMatch(narrative, RISK_RE);
}

function extractFindings(narrative) {
  return safeMatch(narrative, FINDINGS_RE);
}

function splitEvidenceBuckets(narrative) {
  if (!narrative) return { observation_evidence: '', interview_evidence: '', record_review_evidence: '' };
  const observationBits = [], interviewBits = [], recordReviewBits = [];
  const sentences = narrative.split(/\n+/).map(s => normalizeWhitespace(s)).filter(Boolean);
  for (const line of sentences) {
    if (/observation/i.test(line)) observationBits.push(line);
    if (/interview/i.test(line)) interviewBits.push(line);
    if (/review of|record review|electronic health record|ehr|mar|mds|care plan/i.test(line)) recordReviewBits.push(line);
  }
  return {
    observation_evidence: normalizeWhitespace(observationBits.join(' ')),
    interview_evidence: normalizeWhitespace(interviewBits.join(' ')),
    record_review_evidence: normalizeWhitespace(recordReviewBits.join(' ')),
  };
}

function extractResidents(rawBlock) {
  const residentIds = unique(safeMatchAll(rawBlock, RESIDENT_RE));
  if (!residentIds.length) return [];
  return residentIds.map((residentId) => ({
    resident_id: residentId,
    resident_description: '',
    diagnoses_or_conditions: [],
    decision_making_or_communication_status: '',
    citation_specific_issue: '',
    dates_mentioned: unique(safeMatchAll(rawBlock, /\b\d{2}\/\d{2}\/\d{4}\b/g)),
    related_staff: unique(safeMatchAll(rawBlock, /Staff\s+[A-Z]\b/g)),
    evidence: { record_review: '', interviews: '', observations: '' },
  }));
}

function buildFallbackCitation(rawBlock, tag, scopeSeverity = '') {
  const cleanedBlock = cleanBlockAfterSegmentation(rawBlock);
  const narrative = extractNarrative(cleanedBlock);
  const deficiencySummary = extractDeficiencySummary(narrative);
  const evidenceLead = extractEvidenceLead(narrative);
  const sampleScope = extractSampleScope(narrative);
  const riskStatement = extractRiskStatement(narrative);
  const findingsFull = extractFindings(narrative);
  const federalRequirementText = extractFederalRequirementText(cleanedBlock);
  const cfrCitations = unique(safeMatchAll(cleanedBlock, CFR_RE));
  const stateRefs = unique(safeMatchAll(cleanedBlock, STATE_REF_RE));
  const directQuotes = unique(safeMatchAll(cleanedBlock, QUOTE_RE));
  const residents = extractResidents(cleanedBlock);
  const evidenceBuckets = splitEvidenceBuckets(narrative);
  const pageRange = estimatePageRange();

  return {
    tag: tag,
    tag_number: tag,
    scope_severity: extractScopeSeverity(cleanedBlock, scopeSeverity),
    scope_severity_raw: extractScopeSeverity(cleanedBlock, scopeSeverity)
      ? `SS = ${extractScopeSeverity(cleanedBlock, scopeSeverity)}` : '',
    title: extractRegulatoryTitle(cleanedBlock, tag),
    regulatory_title: extractRegulatoryTitle(cleanedBlock, tag),
    cfr_citations: cfrCitations,
    cfr_citation: cfrCitations.join(', '),
    federal_requirement_text: federalRequirementText,
    deficiency_narrative_full: narrative,
    deficiency_statement: deficiencySummary || evidenceLead,
    deficiency_summary: deficiencySummary || evidenceLead,
    sample_scope_text: sampleScope,
    affected_residents: residents,
    affected_residents_detail: residents,
    residents_affected: residents.map(r => r.resident_id).join(', '),
    harm_or_risk_statement: riskStatement,
    findings_full: findingsFull,
    observation_evidence: evidenceBuckets.observation_evidence,
    interview_evidence: evidenceBuckets.interview_evidence,
    record_review_evidence: evidenceBuckets.record_review_evidence,
    observations: [evidenceBuckets.observation_evidence, evidenceBuckets.interview_evidence, evidenceBuckets.record_review_evidence].filter(Boolean).join(' | ').slice(0, 500),
    staff_statements: unique(safeMatchAll(cleanedBlock, /Staff\s+[A-Z][\s\S]*?(?:stated|reported|said)[\s\S]*?\./gi)),
    administrator_statements: unique(safeMatchAll(cleanedBlock, /Administrator[\s\S]*?(?:stated|reported|said)[\s\S]*?\./gi)),
    facility_expectation_statements: unique(safeMatchAll(cleanedBlock, /expectation[\s\S]*?\./gi)),
    policy_or_guideline_references: unique(safeMatchAll(cleanedBlock, /Purple Book[\s\S]*?\./gi)),
    state_regulatory_references: stateRefs,
    direct_quotes: directQuotes,
    deficiency_category: '',
    department_owner: '',
    poc_inputs: {},
    page_start: pageRange.page_start,
    page_end: pageRange.page_end,
    extraction_status: 'fallback',
    extraction_error: '',
    raw_block: cleanedBlock,
  };
}

function isMinimallyUsableCitation(citation) {
  return Boolean(
    citation &&
    citation.tag_number &&
    citation.tag_number !== 'F0000' &&
    (
      (citation.deficiency_narrative_full && citation.deficiency_narrative_full.trim().length >= 40) ||
      (citation.deficiency_summary && citation.deficiency_summary.trim().length >= 25) ||
      (citation.deficiency_statement && citation.deficiency_statement.trim().length >= 25)
    )
  );
}

function segmentCitationBlocks(rawText) {
  const cleaned = cleanForSegmentation(rawText);
  const matches = [...cleaned.matchAll(TAG_START_RE)];
  if (!matches.length) return [];
  const blocks = [];
  // Deduplicate: keep only first occurrence of each tag
  const seen = new Set();
  for (let i = 0; i < matches.length; i++) {
    const current = matches[i];
    const tag = current[2] ? current[2].toUpperCase() : '';
    const scope = current[3] ? current[3].toUpperCase() : '';
    if (!tag || tag === 'F0000') continue;
    if (seen.has(tag)) continue;
    seen.add(tag);
    const start = current.index + (current[1] ? current[1].length : 0);
    // Find next NEW tag (not already seen)
    let end = cleaned.length;
    for (let j = i + 1; j < matches.length; j++) {
      const nextTag = matches[j][2] ? matches[j][2].toUpperCase() : '';
      if (nextTag && nextTag !== 'F0000' && !seen.has(nextTag)) {
        end = matches[j].index;
        break;
      }
    }
    const rawBlock = cleaned.slice(start, end).trim();
    if (!rawBlock) continue;
    blocks.push({ tag_number: tag, scope_severity_hint: scope, raw_block: rawBlock });
  }
  return blocks;
}

async function enrichCitationWithAI(citation, aiExtractor) {
  if (typeof aiExtractor !== 'function') return citation;
  try {
    const aiResult = await aiExtractor({
      tag_number: citation.tag_number,
      raw_block: citation.raw_block,
      fallback: citation,
    });
    if (!aiResult || typeof aiResult !== 'object') {
      return { ...citation, extraction_status: 'fallback', extraction_error: 'empty AI result' };
    }
    const merged = {
      ...citation,
      ...aiResult,
      tag: citation.tag_number,
      tag_number: citation.tag_number,
      raw_block: citation.raw_block,
      extraction_status: 'ai',
      extraction_error: '',
    };
    // Never let AI overwrite these with empty values
    for (const f of ['scope_severity','title','regulatory_title','cfr_citations','cfr_citation','deficiency_statement','deficiency_summary','deficiency_narrative_full']) {
      if (!merged[f] && citation[f]) merged[f] = citation[f];
    }
    if (!isMinimallyUsableCitation(merged)) {
      return { ...citation, extraction_status: 'fallback', extraction_error: 'AI result not minimally usable' };
    }
    return merged;
  } catch (error) {
    return { ...citation, extraction_status: 'fallback', extraction_error: error?.message || 'AI extraction failed' };
  }
}

async function parseCMS2567(rawText, options = {}) {
  const { aiExtractor = null, concurrency = 4 } = options;
  const candidates = segmentCitationBlocks(rawText);
  if (!candidates.length) {
    return { document_type: 'CMS-2567', citations: [], stats: { candidate_count: 0, usable_count: 0, ai_count: 0, fallback_count: 0 } };
  }
  const fallbackCitations = candidates.map(c => buildFallbackCitation(c.raw_block, c.tag_number, c.scope_severity_hint));
  if (!aiExtractor) {
    // Return ALL candidates — include even minimally usable ones, mark others for review
    const allCitations = fallbackCitations.map(c => ({
      ...c,
      requires_human_review: !isMinimallyUsableCitation(c),
    }));
    return {
      document_type: 'CMS-2567',
      citations: allCitations,
      stats: { candidate_count: candidates.length, usable_count: allCitations.filter(isMinimallyUsableCitation).length, ai_count: 0, fallback_count: allCitations.length },
    };
  }
  const enriched = new Array(fallbackCitations.length);
  let index = 0;
  async function worker() {
    while (index < fallbackCitations.length) {
      const i = index++;
      enriched[i] = await enrichCitationWithAI(fallbackCitations[i], aiExtractor);
    }
  }
  const workerCount = Math.min(Math.max(concurrency, 1), fallbackCitations.length);
  await Promise.all(Array.from({ length: workerCount }, () => worker()));
  const allEnriched = enriched.map((c, i) => c || fallbackCitations[i]);
  return {
    document_type: 'CMS-2567',
    citations: allEnriched,
    stats: {
      candidate_count: candidates.length,
      usable_count: allEnriched.filter(isMinimallyUsableCitation).length,
      ai_count: allEnriched.filter(c => c.extraction_status === 'ai').length,
      fallback_count: allEnriched.filter(c => c.extraction_status === 'fallback').length,
    },
  };
}

module.exports = { cleanForSegmentation, cleanBlockAfterSegmentation, segmentCitationBlocks, buildFallbackCitation, isMinimallyUsableCitation, parseCMS2567 };

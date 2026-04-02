/************ CONFIG ************/
const SHEET_NAME = "Properties";
const API_BASE   = "https://data.oregon.gov/resource/tckn-sxa6.json";
const SOCRATA_APP_TOKEN = ""; // optional

// 1-based columns in your sheet
// Owner name fields (from your sheet):
// I = Owner 1 First, J = Owner 1 Last, K = Owner 2 First, L = Owner 2 Last
const COL_O1_FIRST = 9;   // I
const COL_O1_LAST  = 10;  // J
const COL_O2_FIRST = 11;  // K
const COL_O2_LAST  = 12;  // L

// Output columns
const COL_REGISTRY  = 50; // AX
const COL_MEM_FIRST = 51; // AY
const COL_MEM_LAST  = 52; // AZ
const COL_MEM_ADDR  = 53; // BA

// Batch controls
const BATCH_SIZE  = 60;
const MAX_SECONDS = 310;

/************ PRIORITIES ************/
const PERSON_PRIORITY = [
  "MEMBER",
  "MEMBER-MANAGER",
  "MEMBER MANAGER",
  "MANAGER",
  "AUTHORIZED REPRESENTATIVE",
  "PRESIDENT",
  "OWNER",
  "REGISTERED AGENT"
];

const ADDRESS_PRIORITY = [
  "MAILING ADDRESS",
  "PRINCIPAL PLACE OF BUSINESS",
  "PRINCIPAL PLACE OF BUS",
  "PRINCIPAL PLACE OF BUSINESS ADDRESS"
];

/************ ENTITY DETECTION ************/
// If ANY of these appear, we treat it as a non-person owner and try to resolve through Oregon data
const ENTITY_RE =
  /\b(LLC|L\.?L\.?C\.?|INC|INCORPORATED|CORP|CORPORATION|LTD|LIMITED|LP|L\.?P\.?|LLP|TRUST|ESTATE|FAMILY|HOLDINGS|PROPERTIES)\b|\bTR\b\.?/i;

function isEntityName(s) {
  if (s === null || s === undefined) return false;
  const t = String(s).trim();
  if (!t) return false;
  return ENTITY_RE.test(t);
}

/**
 * Build the best “entity query name” from columns I–L on a row.
 * We try combinations first (First+Last) then fallback to just Last.
 * We choose the longest candidate that matches entity keywords.
 */
function getEntityQueryFromRow_(sheet, r) {
  const o1f = sheet.getRange(r, COL_O1_FIRST).getValue();
  const o1l = sheet.getRange(r, COL_O1_LAST ).getValue();
  const o2f = sheet.getRange(r, COL_O2_FIRST).getValue();
  const o2l = sheet.getRange(r, COL_O2_LAST ).getValue();

  const c1 = [o1f, o1l].filter(Boolean).join(" ").trim();
  const c2 = [o2f, o2l].filter(Boolean).join(" ").trim();

  const candidates = [
    c1,
    c2,
    String(o1l || "").trim(),
    String(o2l || "").trim()
  ].filter(x => x && isEntityName(x));

  candidates.sort((a, b) => b.length - a.length);
  return candidates[0] || "";
}

/************ MAIN (BATCHED) ************/
function pullOregonLLCMembers() {
  const started = Date.now();

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(SHEET_NAME);
  if (!sheet) throw new Error(`Tab "${SHEET_NAME}" not found.`);

  const lastRow = sheet.getLastRow();

  // resume pointer
  const props = PropertiesService.getScriptProperties();
  let startRow = Number(props.getProperty("NEXT_ROW") || "2");
  if (startRow < 2) startRow = 2;

  const endRow = Math.min(lastRow, startRow + BATCH_SIZE - 1);

  Logger.log(`Batch startRow=${startRow} endRow=${endRow} lastRow=${lastRow}`);

  const cache = CacheService.getScriptCache();

  for (let r = startRow; r <= endRow; r++) {
    // stop if close to timeout
    if ((Date.now() - started) / 1000 > MAX_SECONDS) {
      props.setProperty("NEXT_ROW", String(r));
      Logger.log(`Stopping early to avoid timeout. NEXT_ROW=${r}`);
      return;
    }

    // NEW: entity query from I–L (not just LLC)
    const entityQueryRaw = getEntityQueryFromRow_(sheet, r);
    const entityQuery = normalize(entityQueryRaw);

    // Only process rows that look like non-person owners (LLC/INC/TRUST/etc.)
    if (!entityQuery) continue;

    // Skip if already filled
    const existingRegistry = sheet.getRange(r, COL_REGISTRY).getValue();
    const existingFirst    = sheet.getRange(r, COL_MEM_FIRST).getValue();
    const existingLast     = sheet.getRange(r, COL_MEM_LAST).getValue();
    const existingAddr     = sheet.getRange(r, COL_MEM_ADDR).getValue();
    if (existingRegistry && (existingFirst || existingLast || existingAddr)) continue;

    try {
      // --- registry lookup (cached) ---
      const regKey = "reginfo:" + entityQuery;
      let info = cache.get(regKey);
      info = info ? JSON.parse(info) : null;

      if (!info) {
        info = findRegistryInfoSmart(entityQuery);
        if (info) cache.put(regKey, JSON.stringify(info), 21600);
      }

      if (!info || !info.registry_number) {
        Logger.log(`Row ${r}: registry not found for "${entityQueryRaw}"`);
        continue;
      }

      sheet.getRange(r, COL_REGISTRY).setValue(String(info.registry_number));

      // --- assoc rows ---
      const assoc = fetchAssociatedRows(info);
      Logger.log(`Row ${r}: entity="${entityQueryRaw}" registry=${info.registry_number} assoc=${assoc.length}`);
      if (!assoc.length) continue;

      const person = pickBestPerson(assoc);
      const addrRow = pickBestAddressRow(assoc);

      const first = person?.first_name ? person.first_name : "";
      const last  = person?.last_name  ? person.last_name  : "";
      const addr  = buildAddress(addrRow) || buildAddress(person) || "";

      sheet.getRange(r, COL_MEM_FIRST).setValue(first);
      sheet.getRange(r, COL_MEM_LAST).setValue(last);
      sheet.getRange(r, COL_MEM_ADDR).setValue(addr);

      SpreadsheetApp.flush();
      Utilities.sleep(350);
    } catch (e) {
      Logger.log(`Row ${r} failed: ${e}`);
    }
  }

  // set next batch
  const next = endRow + 1;
  if (next <= lastRow) {
    props.setProperty("NEXT_ROW", String(next));
    Logger.log(`Batch complete. NEXT_ROW=${next} (run again)`);
  } else {
    props.deleteProperty("NEXT_ROW");
    Logger.log("All done. NEXT_ROW cleared.");
  }
}

/************ DEBUG: run on selected row ************/
function debugActiveRow() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(SHEET_NAME);
  const r = sheet.getActiveRange().getRow();

  const entityQueryRaw = getEntityQueryFromRow_(sheet, r);
  const entityQuery = normalize(entityQueryRaw);

  Logger.log(`Row=${r} Entity(raw)=${entityQueryRaw} Entity(norm)=${entityQuery}`);

  if (!entityQuery) {
    Logger.log("This row does not look like an entity owner (LLC/INC/TRUST/etc.).");
    return;
  }

  const info = findRegistryInfoSmart(entityQuery);
  Logger.log(`RegistryInfo=${JSON.stringify(info)}`);

  if (!info) return;

  const assoc = fetchAssociatedRows(info);
  Logger.log(`AssocCount=${assoc.length}`);
  assoc.slice(0, 20).forEach(x => {
    Logger.log(`${x.associated_name_type} | ${x.first_name || ""} ${x.last_name || ""} | ${buildAddress(x)}`);
  });

  const p = pickBestPerson(assoc);
  const a = pickBestAddressRow(assoc);
  Logger.log(`PickedPerson=${p ? (p.associated_name_type + " " + (p.first_name||"") + " " + (p.last_name||"")) : "NONE"}`);
  Logger.log(`PickedAddr=${buildAddress(a)}`);
}

/************ LOOKUP (SMART) ************/
function findRegistryInfoSmart(entityName) {
  const n = normalize(entityName);

  // Remove common suffix tokens from q (helps matching across LLC/INC/TRUST)
  const qBase = n
    .replace(/\bLLC\b/g, "")
    .replace(/\bINC\b/g, "")
    .replace(/\bINCORPORATED\b/g, "")
    .replace(/\bCORP\b/g, "")
    .replace(/\bCORPORATION\b/g, "")
    .replace(/\bLTD\b/g, "")
    .replace(/\bLIMITED\b/g, "")
    .replace(/\bLLP\b/g, "")
    .replace(/\bLP\b/g, "")
    .replace(/\bTRUST\b/g, "")
    .replace(/\bESTATE\b/g, "")
    .replace(/\bFAMILY\b/g, "")
    .replace(/\s+/g, " ")
    .trim();

  const q = encodeURIComponent(qBase);

  const url =
    `${API_BASE}?` +
    `$select=registry_number,entity_of_record_reg_number,business_name,associated_name_type` +
    `&$q=${q}` +
    `&$limit=50`;

  const rows = fetchJsonWithRetry(url);
  if (!rows.length) return null;

  const targetTokens = significantTokens(n);

  let best = null;
  let bestScore = -1;

  for (const r of rows) {
    const b = normalize(r.business_name || "");
    if (!b) continue;

    const score = tokenScore(targetTokens, significantTokens(b));
    const t = normalizeType(r.associated_name_type || "");
    const bonus = ADDRESS_PRIORITY.includes(t) ? 0.25 : 0;
    const finalScore = score + bonus;

    if (finalScore > bestScore) {
      bestScore = finalScore;
      best = r;
    }
  }

  if (!best || !best.registry_number) return null;

  return {
    registry_number: String(best.registry_number),
    entity_of_record_reg_number: best.entity_of_record_reg_number ? String(best.entity_of_record_reg_number) : "",
    business_name: best.business_name || ""
  };
}

/************ FETCH ASSOCIATIONS ************/
function fetchAssociatedRows(info) {
  // 1) Try entity_of_record_reg_number
  if (info.entity_of_record_reg_number) {
    const url1 =
      `${API_BASE}?` +
      `$select=associated_name_type,first_name,last_name,address,address_continued,city,state,zip,registry_number,entity_of_record_reg_number` +
      `&$where=${encodeURIComponent(`entity_of_record_reg_number='${escapeQuotes(info.entity_of_record_reg_number)}'`)}` +
      `&$limit=500`;

    const r1 = fetchJsonWithRetry(url1);
    if (r1.length) return r1;
  }

  // 2) Fallback to registry_number (quoted)
  const url2 =
    `${API_BASE}?` +
    `$select=associated_name_type,first_name,last_name,address,address_continued,city,state,zip,registry_number,entity_of_record_reg_number` +
    `&$where=${encodeURIComponent(`registry_number='${escapeQuotes(info.registry_number)}'`)}` +
    `&$limit=500`;

  return fetchJsonWithRetry(url2);
}

/************ PICKERS ************/
function pickBestPerson(rows) {
  const candidates = rows.filter(r => (r.first_name || r.last_name) && r.associated_name_type);
  for (const t of PERSON_PRIORITY) {
    const found = candidates.find(r => normalizeType(r.associated_name_type) === t);
    if (found) return found;
  }
  return candidates[0] || null;
}

function pickBestAddressRow(rows) {
  const candidates = rows.filter(r => r.address && r.associated_name_type);
  for (const t of ADDRESS_PRIORITY) {
    const found = candidates.find(r => normalizeType(r.associated_name_type) === t);
    if (found) return found;
  }
  return candidates[0] || null;
}

/************ TOKEN MATCH UTILS ************/
function significantTokens(s) {
  const stop = new Set([
    "LLC","L.L.C.","THE","AND","OF","CO","COMPANY",
    "INC","INC.","INCORPORATED","CORP","CORPORATION",
    "LP","LLP","LTD","LIMITED","TRUST","ESTATE","FAMILY"
  ]);

  return normalize(s)
    .replace(/[^\w\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .split(" ")
    .filter(t => t && !stop.has(t));
}

function tokenScore(aTokens, bTokens) {
  if (!aTokens.length || !bTokens.length) return 0;
  const b = new Set(bTokens);
  let hit = 0;
  for (const t of aTokens) if (b.has(t)) hit++;
  return hit / Math.max(aTokens.length, 3);
}

/************ GENERAL UTILS ************/
function buildAddress(r) {
  if (!r) return "";
  const parts = [r.address, r.address_continued, r.city, r.state, r.zip].filter(Boolean);
  return parts.join(", ");
}

function normalize(v) {
  return String(v || "").trim().replace(/\s+/g, " ").toUpperCase();
}

function normalizeType(v) {
  return normalize(v).replace(/\s+/g, " ");
}

function escapeQuotes(s) {
  return String(s || "").replace(/'/g, "''");
}

function fetchJsonWithRetry(url) {
  const headers = {};
  if (SOCRATA_APP_TOKEN) headers["X-App-Token"] = SOCRATA_APP_TOKEN;

  for (let attempt = 1; attempt <= 3; attempt++) {
    const resp = UrlFetchApp.fetch(url, { muteHttpExceptions: true, headers });
    const code = resp.getResponseCode();
    const text = resp.getContentText();

    if (code >= 200 && code < 300) return JSON.parse(text);

    Logger.log(`HTTP ${code} attempt ${attempt} url=${url}`);
    Logger.log(text.slice(0, 250));

    Utilities.sleep(700 * attempt);
  }
  return [];
}

/************ RESET POINTER ************/
function resetBatchPointer() {
  PropertiesService.getScriptProperties().deleteProperty("NEXT_ROW");
  Logger.log("NEXT_ROW cleared. Next run will start from row 2.");
}

/************ FILL ACTIVE ROW NOW ************/
function fillActiveRowNow() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(SHEET_NAME);
  const r = sheet.getActiveRange().getRow();

  const entityQueryRaw = getEntityQueryFromRow_(sheet, r);
  const entityQuery = normalize(entityQueryRaw);

  if (!entityQuery) throw new Error("This row does not look like an entity owner.");

  const info = findRegistryInfoSmart(entityQuery);
  if (!info || !info.registry_number) throw new Error("No registry found");

  const assoc = fetchAssociatedRows(info);
  if (!assoc.length) throw new Error("No associated rows found");

  const person = pickBestPerson(assoc);
  const addrRow = pickBestAddressRow(assoc);

  sheet.getRange(r, COL_REGISTRY).setValue(info.registry_number);
  sheet.getRange(r, COL_MEM_FIRST).setValue(person?.first_name || "");
  sheet.getRange(r, COL_MEM_LAST).setValue(person?.last_name || "");
  sheet.getRange(r, COL_MEM_ADDR).setValue(buildAddress(addrRow) || "");

  SpreadsheetApp.flush();
}

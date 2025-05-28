import xlsx from 'xlsx';
import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
// At the very top of scripts/import_venues.js
import path from 'path';
import { fileURLToPath } from 'url';

// Recreate __dirname in ESM
const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);

// …then later…
const filePath = path.resolve(__dirname, '../excels/canonical_venues.xlsx');
dotenv.config();

// 1) Supabase init (SERVICE key şart)
const sb = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY
);

// 2) Excel oku (sheet isimleri sabit)
const wb = xlsx.readFile(filePath);
console.log('Sheets:', wb.SheetNames);

const canonS   = wb.Sheets['Canonical'];              // corrected | city | capacity
const mapS     = wb.Sheets['Mapping'];                // raw_name | canonical_name
console.log('Found Canonical sheet?', !!canonS);
console.log('Found Mapping sheet?'  , !!mapS);

if (!canonS) throw new Error('Sheet "Canonical" not found');
if (!mapS)   throw new Error('Sheet "Mapping" not found');

const canonRows = xlsx.utils.sheet_to_json(canonS, { defval: '' });
const mapRows   = xlsx.utils.sheet_to_json(mapS,   { defval: '' });


// 3) Canonical upsert
const idCache = new Map();  // name(lower) -> id

for (const r of canonRows) {
  const name     = r.corrected?.trim();
  if (!name) continue;

  const city     = r.city?.trim() || null;
  const capacity = r.capacity ? parseInt(r.capacity, 10) : null;
  const key = name.toLowerCase();

  if (idCache.has(key)) {
    console.log(`[SKIP canonical] already have "${name}"`);
    continue;
  }

  const { data, error } = await sb
    .from('canonical_venues')
    .upsert({ name, city, capacity }, { onConflict: 'name' })
    .select('id')
    .single();

  if (error) throw error;
  idCache.set(name.toLowerCase(), data.id);
}

console.log(`✅ canonical_venues upsert → ${idCache.size} kayıt`);

// 4) Manual mapping upsert
const seenRaw = new Set();
let inserted = 0;

for (const r of mapRows) {
  const raw   = r.raw_name?.trim().toLowerCase();
  const canon = (r.canonical_name?.trim() || r.raw_name?.trim()).toLowerCase();
  if (!raw || !canon) continue;

  if (seenRaw.has(raw)) {
    console.log(`[SKIP mapping] duplicate raw_name "${raw}"`);
    continue;
  }
  seenRaw.add(raw);

  const cid = idCache.get(canon);
  if (!cid) {
    console.log(`[WARN mapping] no canonical for "${canon}"`);
    continue;
  }

  const { error } = await sb
    .from('manual_venue_map')
    .upsert({ raw_name: raw, canonical_id: cid });

  if (error) throw error;
  inserted++;
}

console.log(`✅ manual_venue_map upsert → ${inserted} satır`);

process.exit(0);
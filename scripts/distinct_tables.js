import { createClient } from '@supabase/supabase-js'
import dotenv from 'dotenv'
import xlsx from 'xlsx'
import stringSimilarity from 'string-similarity'

dotenv.config()

// Manual venue mapping: exact overrides for known variations
const manualVenueMap = {
  // e.g. 'istanbul jj arena': 'jj arena',
};

// Create a single supabase client for interacting with your database
const supabase = createClient('https://wpydilkmtmgbunectxpx.supabase.co', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndweWRpbGttdG1nYnVuZWN0eHB4Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDc2Njc0MjUsImV4cCI6MjA2MzI0MzQyNX0.OXl2AHppGSFWSGypG_2_NZj7atD_gM_U2I7oAkpTB5w')

const tables = ['bugece_events', 'biletinial_events', 'passo_events', 'biletix_events'];

const venueMap = {};

// Fetch distinct venues per table
for (const table of tables) {
  const { data, error } = await supabase
    .from(table)
    .select('venue', { count: 'exact', head: false });

  if (error) {
    console.error(`Error fetching from ${table}:`, error);
    continue;
  }

  venueMap[table] = [...new Set(data.map(row => row.venue).filter(Boolean))];
}

// Build merged rows for original
const maxRows = Math.max(...Object.values(venueMap).map(arr => arr.length));
const originalRows = Array.from({ length: maxRows }, (_, i) => {
  const row = {};
  for (const table of tables) {
    row[table] = venueMap[table][i] || '';
  }
  return row;
});

// Collect all distinct names globally
const allDistinct = [...new Set(Object.values(venueMap).flat())];

// Build canonical map using manual overrides and fuzzy matching
const canonicalMap = {};
allDistinct.forEach(name => {
  const lowerName = name.toLowerCase();
  if (manualVenueMap[lowerName]) {
    canonicalMap[name] = manualVenueMap[lowerName];
  } else {
    const { bestMatch } = stringSimilarity.findBestMatch(lowerName, allDistinct.map(n => n.toLowerCase()));
    const matchName = allDistinct.find(n => n.toLowerCase() === bestMatch.target);
    if (bestMatch.rating > 0.8 && matchName && matchName.toLowerCase() !== lowerName) {
      // Choose the shorter string as canonical
      canonicalMap[name] = matchName.length < name.length ? matchName : name;
    } else {
      canonicalMap[name] = name;
    }
  }
});

// Build standardized rows by replacing with canonical names
const standardizedRows = originalRows.map(row => {
  const newRow = {};
  for (const table of tables) {
    const orig = row[table];
    newRow[table] = orig ? canonicalMap[orig] || orig : '';
  }
  return newRow;
});

// Write both Excel files
const workbookOriginal = xlsx.utils.book_new();
const wsOrig = xlsx.utils.json_to_sheet(originalRows);
xlsx.utils.book_append_sheet(workbookOriginal, wsOrig, 'Original_Venues');
xlsx.writeFile(workbookOriginal, 'original_venues.xlsx');

const workbookStandard = xlsx.utils.book_new();
const wsStd = xlsx.utils.json_to_sheet(standardizedRows);
xlsx.utils.book_append_sheet(workbookStandard, wsStd, 'Standardized_Venues');
xlsx.writeFile(workbookStandard, 'standardized_venues.xlsx');

console.log('Excel files written: original_venues.xlsx, standardized_venues.xlsx');

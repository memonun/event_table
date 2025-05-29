import { serve } from "https://deno.land/std@0.202.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

// Allow use of Deno global without errors
declare const Deno: any;

/**
 * Helper: strip accents/diacritics, lowercase & trim.
 * Example: "İstanbul" → "istanbul"
 */
function normalizeVenue(str: string): string {
  return str
    .trim()
    .toLowerCase()
    .normalize("NFD")                 // split letter + diacritic
    .replace(/[\u0300-\u036f]/g, ""); // remove diacritics
}

serve(async (req) => {
  try {
    // --- 1. Payload parse ---
    const { table, event_id, raw_name } = await req.json();
    if (!table || !event_id || !raw_name) {
      return new Response(
        JSON.stringify({ error: "Missing table, event_id or raw_name" }),
        { status: 400 },
      );
    }

    // --- 2. Supabase init ---
    const sb = createClient(
      Deno.env.get("URL")!,
      Deno.env.get("SERVICE_ROLE_KEY")!,
    );

    // --- 3. Normalise raw text ---
    const raw = normalizeVenue(raw_name);
    let canonical_id: string | null = null;

    // --- 4. Manual alias lookup ---
    const { data: manual, error: mErr } = await sb
      .from("manual_venue_map")
      .select("canonical_id")
      .eq("raw_name", raw)
      .maybeSingle();
    if (mErr) console.error("Manual lookup error:", mErr);
    canonical_id = manual?.canonical_id ?? null;

    // --- 5. Fuzzy lookup ---
    if (!canonical_id) {
      const { data: best, error: fErr } = await sb.rpc(
        "match_canonical_venue",
        { input_name: raw, threshold: 0.75 },
      ).maybeSingle();
      if (fErr) console.error("Fuzzy RPC error:", fErr);
      canonical_id = best?.canonical_id ?? null;
    }

    // --- 6. Write back or queue unmatched ---
    if (canonical_id) {
      const { error: uErr } = await sb
        .from(table)
        .update({ canonical_venue_id: canonical_id })
        .eq("id", event_id);
      if (uErr) {
        console.error("Update event error:", uErr);
        return new Response(JSON.stringify({ error: uErr.message }), {
          status: 500,
        });
      }
    } else {
      const { error: qErr } = await sb
        .from("unmatched_venues")
        .insert({ raw_name: raw, provider: table });
      if (qErr) console.error("Log unmatched error:", qErr);
    }

    return new Response(
      JSON.stringify({
        canonical_id,
        processed_at: new Date().toISOString(),
      }),
      { status: 200 },
    );
  } catch (err: any) {
    console.error("Unexpected error:", err);
    return new Response(JSON.stringify({ error: err.message }), {
      status: 500,
    });
  }
});
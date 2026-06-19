"""
Vectis Post-Ingest Intelligence Engine.

Runs as a standalone re-classification pass against permits already stored in Supabase.
Targets any record where complexity_tier = 'Unknown' and upgrades it in-place.

Four-level strategy:
- Level 1 (Keyword Turbo): Free keyword filter — classifies obvious Commodity permits instantly.
- Level 2 (Pattern Grouping): Collapses duplicate descriptions so each unique pattern is only
  sent to the AI once, reducing API cost.
- Level 3 (AI Processor): Sends batches of unique patterns to Gemini 2.0 Flash for nuanced
  classification into 'Strategic', 'Commodity', or 'Ambiguous'.
- Level 4 (Broadcast Updates): Applies AI results back to all DB records sharing each pattern
  via a single batched upsert per batch, scoped by (city, permit_id).

Note: This engine uses a 3-tier taxonomy ('Strategic', 'Commodity', 'Ambiguous') aligned with
the classify_engine prompt. The main orchestrator (ingest_velocity_50.py) uses a separate
4-tier taxonomy ('Commercial', 'Residential', 'Commodity', 'Unknown'). Records classified by
the orchestrator at ingest time will not be re-processed here.
"""
import os
import json
import time
from dotenv import load_dotenv
from supabase import create_client, Client
import google.genai as genai
from google.genai import types

# --- 1. CONFIGURATION ---
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

BATCH_SIZE = 15     # Unique patterns to send to AI at once
UPSERT_CHUNK = 200  # Max records per Supabase upsert call

# Initialize Supabase
if not SUPABASE_URL or not SUPABASE_KEY:
    print("!! ERROR: Supabase credentials missing.")
    exit(1)
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Initialize Gemini (soft-fail so pipeline survives missing key)
if GOOGLE_API_KEY:
    ai_client = genai.Client(api_key=GOOGLE_API_KEY)
else:
    ai_client = None
    print("GOOGLE_API_KEY missing. AI features will be disabled.")


# --- LEVEL 1: FREE KEYWORD CLASSIFIER ---
def run_keyword_turbo():
    """
    Aggressively classifies Commodity items (Roofs, Fences, Pools) 
    using simple Python string matching. Costs $0.
    """
    print("\n>> 🚀 LEVEL 1: Running Keyword Turbo...")
    
    # 1. Fetch unclassified permits
    # We fetch a larger batch (1000) because Python processing is instant/free
    try:
        response = supabase.table('permits')\
            .select('city, permit_id, description, valuation')\
            .eq('complexity_tier', 'Unknown')\
            .limit(1000)\
            .execute()

        permits = response.data
    except Exception as e:
        print(f"   !! Supabase Fetch Error: {e}")
        return

    if not permits:
        print("   No unclassified permits found to process.")
        return

    updates = []
    
    # Keywords that definitively mark a permit as Commodity (Residential/Minor)
    # Based on Master Context "Commodity" definition
    commodity_markers = [
        'roof', 'fence', 'pool', 'spa', 'solar', 'water heater', 
        'driveway', 'patio', 'siding', 'window', 'door', 'hvac', 
        'irrigation', 'sprinkler', 'demolition', 'residential',
        're-roof', 'carport', 'shed', 'deck'
    ]
    
    count = 0
    for p in permits:
        desc = (p['description'] or "").lower()
        val = float(p['valuation'] or 0)

        # Rule: If it matches a marker AND is under $100k, it is Commodity
        # This prevents accidental classification of massive commercial pool complexes
        if any(marker in desc for marker in commodity_markers) and val < 100000:
            updates.append({'city': p['city'], 'permit_id': p['permit_id'], 'complexity_tier': 'Commodity'})
            count += 1

    if updates:
        print(f"   Found {len(updates)} Commodity permits. Upserting to Supabase...")
        for j in range(0, len(updates), UPSERT_CHUNK):
            chunk = updates[j:j + UPSERT_CHUNK]
            try:
                supabase.table('permits').upsert(chunk, on_conflict='city, permit_id').execute()
            except Exception as e:
                print(f"   !! Upsert chunk {j // UPSERT_CHUNK + 1} failed: {e}")
        print(f"   Level 1 Complete. ({count} records classified)")
    else:
        print("   No keyword matches found in this batch.")


# --- LEVEL 2 & 3: PATTERN GROUPING & AI ---
def run_ai_processor():
    """
    Groups unique descriptions and sends to Gemini with Negative Constraints.
    """
    print("\n>> 🧠 LEVEL 2: Starting AI Pattern Processor...")
    
    if not ai_client:
        print("   AI client not initialized. Skipping.")
        return

    # 1. Fetch remaining unclassified permits
    try:
        response = supabase.table('permits')\
            .select('city, permit_id, description, valuation')\
            .eq('complexity_tier', 'Unknown')\
            .limit(500)\
            .execute()
        raw_permits = response.data
    except Exception as e:
        print(f"   !! Supabase Fetch Error: {e}")
        return
    
    if not raw_permits:
        print("   All permits classified! Exiting.")
        return

    # 2. PATTERN GROUPING (The Solopreneur Optimization)
    # We group by description text so we only pay to classify it once.
    # Key includes city to prevent cross-city ID collisions during broadcast updates.
    grouped_patterns = {}
    for p in raw_permits:
        desc_clean = (p['description'] or "").strip()
        if not desc_clean:
            continue

        if desc_clean not in grouped_patterns:
            grouped_patterns[desc_clean] = {
                "sample_records": [],  # list of (city, permit_id) tuples
                "avg_val": 0,
                "count": 0
            }

        grouped_patterns[desc_clean]["sample_records"].append((p['city'], p['permit_id']))
        grouped_patterns[desc_clean]["count"] += 1

        # Track max valuation to help AI context
        curr_val = float(p['valuation'] or 0)
        if curr_val > grouped_patterns[desc_clean]["avg_val"]:
            grouped_patterns[desc_clean]["avg_val"] = curr_val

    # Convert to list for batching
    unique_patterns = list(grouped_patterns.keys())
    print(f"   📉 Optimization: Collapsed {len(raw_permits)} rows into {len(unique_patterns)} unique patterns.")

    # 3. Process in Batches
    for i in range(0, len(unique_patterns), BATCH_SIZE):
        batch_keys = unique_patterns[i : i + BATCH_SIZE]
        
        # Construct Payload for AI
        ai_payload = []
        for desc in batch_keys:
            ai_payload.append({
                "description": desc,
                "max_valuation": grouped_patterns[desc]["avg_val"]
            })

        print(f"   ... Sending Batch {i//BATCH_SIZE + 1} to Gemini ({len(batch_keys)} patterns)...")

        # 4. SYSTEM INSTRUCTION with CRITICAL NEGATIVE CONSTRAINTS
        prompt = f"""
        You are a Civil Engineering Risk Analyst.
        Classify the following permit descriptions into strictly one of these tiers:
        
        1. 'Strategic': New Commercial construction, Multi-family, Commercial Remodels (TI), Medical, Industrial.
        2. 'Commodity': Residential work, Roofs, Pools, Fences, Signs, Repairs, Single Family Additions.
        3. 'Ambiguous': Vague data (e.g. "Building Permit") or missing context.

        ### CRITICAL NEGATIVE CONSTRAINTS (Prevent Hallucinations)
        - If the description contains "bedroom", "house", "residence", "home", or "ADU", it is 'Commodity' even if it mentions "office" (e.g. "Home Office").
        - "Tenant Finish Out" is 'Strategic' ONLY if clearly commercial context (e.g. "Suite 100", "Retail").
        - "Sign" or "Wall Sign" is always 'Commodity' unless valuation > $50,000.
        
        ### DATA
        {json.dumps(ai_payload)}

        ### OUTPUT
        Return strictly a JSON list of objects:
        [{{"description": "...", "tier": "..."}}]
        """

        try:
            response = ai_client.models.generate_content(
                model="gemini-2.0-flash",
                contents=prompt,
                config=types.GenerateContentConfig(response_mime_type="application/json")
            )
            results = json.loads(response.text)

            # Collect all updates from this batch then upsert in one shot
            broadcast_updates = []
            for res in results:
                target_desc = res.get("description")
                tier = res.get("tier")
                if target_desc in grouped_patterns and tier:
                    target_records = grouped_patterns[target_desc]["sample_records"]
                    for city, pid in target_records:
                        broadcast_updates.append({
                            'city': city,
                            'permit_id': pid,
                            'complexity_tier': tier
                        })
                    print(f"      Mapped '{target_desc[:30]}...' -> {tier} "
                          f"({len(target_records)} records)")

            for j in range(0, len(broadcast_updates), UPSERT_CHUNK):
                chunk = broadcast_updates[j:j + UPSERT_CHUNK]
                try:
                    supabase.table('permits').upsert(chunk, on_conflict='city, permit_id').execute()
                except Exception as ue:
                    print(f"   !! Broadcast upsert chunk failed: {ue}")

            time.sleep(2)

        except Exception as e:
            print(f"   !! Batch Error: {e}")
            time.sleep(5)


# --- MAIN EXECUTION (SAFE MODE) ---
if __name__ == "__main__":
    print(">> Starting Vectis Intelligence Engine (SAFE MODE)...")
    
    # STEP 1: Always Run Keyword Turbo (Free & Safe)
    run_keyword_turbo()
    
    # STEP 2: Circuit Breaker for AI
    # This try/except block ensures the pipeline survives if Google returns 403/Restricted
    try:
        # Check if we have a key before even trying
        if ai_client:
            print("\n>> Attempting to connect to Gemini...")
            run_ai_processor()
        else:
            print("\n>> 🧠 Skipping AI Processor (No Key Found).")
            
    except Exception as e:
        print(f"   !! AI Module Skipped due to API restriction/Error: {e}")
        print("   (The pipeline continued successfully without AI)")

    print(">> Engine Cycle Complete.")
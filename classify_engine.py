import os
import json
import time
from dotenv import load_dotenv
from supabase import create_client, Client
import google.generativeai as genai

# --- 1. CONFIGURATION ---
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

# Solopreneur Cost Controls
BATCH_SIZE = 15          # Unique patterns to send to AI at once
MAX_RETRIES = 3

# Initialize Supabase
if not SUPABASE_URL or not SUPABASE_KEY:
    print("!! ERROR: Supabase credentials missing.")
    exit(1)
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Initialize Gemini (Soft Fail if missing, for Safe Mode)
if GOOGLE_API_KEY:
    genai.configure(api_key=GOOGLE_API_KEY)
    # Use the cost-effective Flash model
    model = genai.GenerativeModel('gemini-1.5-flash')
else:
    model = None
    print("⚠️ GOOGLE_API_KEY missing. AI features will be disabled.")


# --- LEVEL 1: FREE KEYWORD CLASSIFIER ---
def run_keyword_turbo():
    """
    Aggressively classifies Commodity items (Roofs, Fences, Pools) 
    using simple Python string matching. Costs $0.
    """
    print("\n>> 🚀 LEVEL 1: Running Keyword Turbo...")
    
    # 1. Fetch unclassified 'Standard' permits
    # We fetch a larger batch (1000) because Python processing is instant/free
    try:
        response = supabase.table('permits')\
            .select('permit_id, description, valuation')\
            .eq('complexity_tier', 'Standard')\
            .limit(1000)\
            .execute()
        
        permits = response.data
    except Exception as e:
        print(f"   !! Supabase Fetch Error: {e}")
        return

    if not permits:
        print("   No 'Standard' permits found to process.")
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
            updates.append({'permit_id': p['permit_id'], 'complexity_tier': 'Commodity'})
            count += 1

    # Bulk Update
    if updates:
        print(f"   Found {len(updates)} Commodity permits. Updating Supabase...")
        
        # We loop updates to ensure reliability. 
        # (Supabase bulk update via exact match is complex in Python client, loop is safe for batch jobs)
        for item in updates:
            try:
                supabase.table('permits').update({'complexity_tier': item['complexity_tier']})\
                    .eq('permit_id', item['permit_id']).execute()
            except Exception as e:
                print(f"   !! Update failed for {item['permit_id']}: {e}")
                
        print(f"   ✅ Level 1 Complete. ({count} records classified)")
    else:
        print("   No Keyword matches found in this batch.")


# --- LEVEL 2 & 3: PATTERN GROUPING & AI ---
def run_ai_processor():
    """
    Groups unique descriptions and sends to Gemini with Negative Constraints.
    """
    print("\n>> 🧠 LEVEL 2: Starting AI Pattern Processor...")
    
    if not model:
        print("   ⚠️ AI Model not initialized. Skipping.")
        return

    # 1. Fetch remaining Standard permits
    try:
        response = supabase.table('permits')\
            .select('permit_id, description, valuation')\
            .eq('complexity_tier', 'Standard')\
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
    # We group by the Description text so we only pay to classify it once.
    grouped_patterns = {}
    for p in raw_permits:
        desc_clean = (p['description'] or "").strip()
        if not desc_clean: continue
        
        if desc_clean not in grouped_patterns:
            grouped_patterns[desc_clean] = {
                "sample_ids": [], 
                "avg_val": 0, 
                "count": 0
            }
        
        grouped_patterns[desc_clean]["sample_ids"].append(p['permit_id'])
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
            # Call Gemini
            response = model.generate_content(prompt)
            
            # Clean potential markdown (```json ... ```)
            clean_text = response.text.replace("```json", "").replace("```", "").strip()
            results = json.loads(clean_text)

            # 5. BROADCAST UPDATES (Level 4)
            for res in results:
                target_desc = res.get("description")
                tier = res.get("tier")
                
                if target_desc in grouped_patterns and tier:
                    # Get all Permit IDs that match this description
                    target_ids = grouped_patterns[target_desc]["sample_ids"]
                    
                    # Update all of them
                    for pid in target_ids:
                        supabase.table('permits').update({'complexity_tier': tier})\
                            .eq('permit_id', pid).execute()
                    
                    print(f"      Mapped '{target_desc[:30]}...' -> {tier} ({len(target_ids)} records updated)")

            time.sleep(2) # Respect rate limits

        except Exception as e:
            print(f"   !! Batch Error (Likely Safety/Auth): {e}")
            # We do NOT exit here, we just skip this batch so the loop continues
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
        if GOOGLE_API_KEY:
            print("\n>> 🧠 Attempting to connect to Gemini...")
            run_ai_processor()
        else:
            print("\n>> 🧠 Skipping AI Processor (No Key Found).")
            
    except Exception as e:
        print(f"   !! AI Module Skipped due to API restriction/Error: {e}")
        print("   (The pipeline continued successfully without AI)")

    print(">> Engine Cycle Complete.")
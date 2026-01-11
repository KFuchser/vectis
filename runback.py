import os
import json
import time
from supabase import create_client, Client
from dotenv import load_dotenv
from google import genai 
from google.genai import types

load_dotenv()

# --- CONFIG ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
GEMINI_KEY = os.getenv("GOOGLE_API_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
ai_client = genai.Client(api_key=GEMINI_KEY)

def batch_classify_backlog(records):
    if not records: return []
    
    # NEW: Capture BOTH permit_id AND city to satisfy Postgres constraints
    temp_map = {i: {"id": r['permit_id'], "city": r['city']} for i, r in enumerate(records)}
    
    batch_prompt = """
    Classify these permits into {id, tier, category, reason}.
    Tiers: 'Strategic' or 'Commodity'.
    """
    for idx, r in enumerate(records):
        desc = r.get('description', 'No Description')[:200]
        batch_prompt += f"ID {idx}: Val=${r.get('valuation')} | Desc: {desc}\n"

    try:
        response = ai_client.models.generate_content(
            model="gemini-2.0-flash",
            contents=batch_prompt,
            config=types.GenerateContentConfig(response_mime_type="application/json")
        )
        
        raw_text = response.text.strip()
        if "```json" in raw_text:
            raw_text = raw_text.split("```json")[-1].split("```")[0].strip()
            
        data_list = json.loads(raw_text)
        if isinstance(data_list, dict): data_list = data_list.get("results", [])
        
        updates = []
        for res in data_list:
            try:
                idx = int(res.get("id"))
                if idx in temp_map:
                    updates.append({
                        "permit_id": temp_map[idx]["id"], # The Unique Key
                        "city": temp_map[idx]["city"],   # SATISFIES NOT-NULL CONSTRAINT
                        "complexity_tier": "Strategic" if "STRATEGIC" in str(res.get("tier")).upper() else "Commodity",
                        "project_category": res.get("category"),
                        "ai_rationale": res.get("reason", "Manual Backfill Cleanup")
                    })
            except: continue
        return updates
    except Exception as e:
        print(f"⚠️ AI Error: {e}")
        return []

def run_cleanup():
    # Targets records where the category is missing
    print("🔍 Searching for records needing classification...")
    response = supabase.table('permits').select("*").is_("project_category", "null").limit(100).execute()
    records = response.data
    
    if not records:
        print("✅ All clear! No backlog found.")
        return False

    all_updates = []
    for i in range(0, len(records), 20):
        chunk = records[i:i+20]
        updates = batch_classify_backlog(chunk)
        if updates: all_updates.extend(updates)
        time.sleep(1)

    if all_updates:
        print(f"💾 Bulk updating {len(all_updates)} records...")
        supabase.table('permits').upsert(all_updates, on_conflict='permit_id').execute()
    return True

if __name__ == "__main__":
    # We run 10 cycles to clear the 1,000 LA records
    for i in range(10):
        print(f"--- Cycle {i+1} ---")
        if not run_cleanup(): break
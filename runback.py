import os
import json
import time
from supabase import create_client, Client
from dotenv import load_dotenv
import google.genai as genai
from google.genai import types
from service_models import PermitRecord, ComplexityTier, ProjectCategory

load_dotenv()

# --- CONFIG ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
GEMINI_KEY = os.getenv("GOOGLE_API_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
ai_client = genai.Client(api_key=GEMINI_KEY)

def batch_classify_backlog(records):
    """
    Standalone AI Classifier for Backfilling.
    """
    print(f"🛰️ Processing Batch of {len(records)}...")
    
    batch_prompt = """
    You are a Permit Classification Engine. 
    Classify these permits into a 'tier' (Strategic, Commodity) and 'category'.
    
    CRITICAL NEGATIVE CONSTRAINTS:
    1. IF description has 'office', 'studio', or 'shed' BUT is in a residential context (backyard, house, garage), CLASSIFY AS COMMODITY / RESIDENTIAL_ALTERATION.
    2. 'Strategic' is ONLY for: New Commercial Buildings, Retail (Starbucks, etc), Multifamily (>4 units), Industrial.
    3. 'Commodity' includes: All single-family residential (even extensive remodels), Signs, Pools.

    Return valid JSON list of objects: {id, tier, category, reason}
    
    Valid Categories: 
    - Residential - New Construction
    - Residential - Alteration/Addition
    - Commercial - New Construction
    - Commercial - Tenant Improvement
    - Infrastructure/Public Works
    
    INPUT DATA:
    """
    
    # Create a mapping to find the real database ID later
    temp_map = {i: r['permit_id'] for i, r in enumerate(records)}
    
    for idx, r in enumerate(records):
        desc = r.get('description', 'No Description')[:200].replace("\n", " ")
        val = r.get('valuation', 0)
        batch_prompt += f"ID {idx}: Val=${val} | Desc: {desc}\n"

    try:
        response = ai_client.models.generate_content(
            model="gemini-2.0-flash",
            contents=batch_prompt,
            config=types.GenerateContentConfig(response_mime_type="application/json")
        )
        
        results = json.loads(response.text)
        data_list = results.get("results") if isinstance(results, dict) else results
        
        updates = []
        for res in data_list:
            try:
                idx = int(res.get("id"))
                if idx in temp_map:
                    permit_id = temp_map[idx]
                    
                    # Normalization Logic
                    raw_tier = str(res.get("tier", "Commodity")).upper()
                    tier = "Strategic" if "STRATEGIC" in raw_tier else "Commodity"
                    
                    cat = res.get("category", "Residential - Alteration/Addition")
                    reason = res.get("reason", "Backfill AI")
                    
                    updates.append({
                        "permit_id": permit_id,
                        "complexity_tier": tier,
                        "project_category": cat,
                        "ai_rationale": reason
                    })
            except:
                continue
                
        return updates

    except Exception as e:
        print(f"⚠️ AI Error: {e}")
        return []

def run_backfill():
    print("🚀 Starting AI Backfill Operation...")
    
    # 1. Fetch Unclassified Records
    # We look for records where project_category is NULL
    print("🔍 Scanning for unclassified records...")
    response = supabase.table('permits').select("*").is_("project_category", "null").limit(500).execute()
    records = response.data
    
    if not records:
        print("✅ No unclassified records found! The database is clean.")
        return

    print(f"Found {len(records)} records pending classification.")
    
    # 2. Process in Batches
    batch_size = 20
    for i in range(0, len(records), batch_size):
        chunk = records[i:i + batch_size]
        
        # Run AI
        updates = batch_classify_backlog(chunk)
        
        # Save to DB
        if updates:
            print(f"💾 Saving {len(updates)} updates to Supabase...")
            for update in updates:
                # We have to update one by one or use a more complex upsert. 
                # For safety/simplicity in this script, we update one by one.
                supabase.table('permits').update({
                    "complexity_tier": update['complexity_tier'],
                    "project_category": update['project_category'],
                    "ai_rationale": update['ai_rationale']
                }).eq("permit_id", update['permit_id']).execute()
        
        # Rate Limit Safety
        time.sleep(1)

if __name__ == "__main__":
    # Loop to process multiple chunks if needed
    for _ in range(5): # Run 5 cycles of 500 records (2500 total) per execution
        run_backfill()
        time.sleep(2)
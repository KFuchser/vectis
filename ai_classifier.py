import os
import json
import time
import re
from dotenv import load_dotenv
from supabase import create_client, Client
import google.generativeai as genai

# 1. Load Config
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

if not GOOGLE_API_KEY:
    print("!! ERROR: GOOGLE_API_KEY is missing from .env")
    exit(1)

genai.configure(api_key=GOOGLE_API_KEY)

# CRITICAL FIX: Use the 'Lite' model which appears in your allowed list
# This should have a fresh quota bucket separate from the main Flash model.
MODEL_NAME = "gemini-2.0-flash-exp" 
BATCH_SIZE = 15

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

SYSTEM_INSTRUCTION = """
You are a Civil Engineering Data Analyst.
Classify the following permit descriptions into one of three tiers:

1. 'Strategic': New commercial construction, multi-family, large additions, infrastructure, medical, high-value renovations.
2. 'Commodity': Simple repairs, fences, roofs, water heaters, signs, pools, single-family alterations.
3. 'Standard': Ambiguous or missing data.

Return strictly a JSON list of objects:
[{"permit_id": "...", "new_tier": "...", "reasoning": "..."}]
"""

def fetch_unclassified_permits(limit=50):
    try:
        response = supabase.table('permits')\
            .select('permit_id, description, valuation, city')\
            .eq('complexity_tier', 'Standard')\
            .limit(limit)\
            .execute()
        return response.data
    except Exception as e:
        print(f"!! Supabase Error: {e}")
        return []

def run_classification_job():
    print(f">> Starting AI Batch Processor using {MODEL_NAME}...")
    print(f">> Batch Size: {BATCH_SIZE}")
    print(">> Press Ctrl+C to stop.\n")
    
    total_processed = 0
    
    while True:
        # 1. Fetch Batch
        permits = fetch_unclassified_permits(limit=BATCH_SIZE)
        
        if not permits:
            print(">> All 'Standard' permits have been classified! Exiting.")
            break

        # 2. Prepare Payload
        payload_text = json.dumps([
            {"permit_id": p['permit_id'], "desc": p['description'], "val": p['valuation']} 
            for p in permits
        ], indent=2)

        print(f">> Processing batch of {len(permits)}...")

        # 3. Call Gemini
        try:
            model = genai.GenerativeModel(
                model_name=MODEL_NAME,
                system_instruction=SYSTEM_INSTRUCTION,
                generation_config={"response_mime_type": "application/json"}
            )
            
            response = model.generate_content(f"Analyze this data:\n{payload_text}")
            clean_text = response.text.replace("```json", "").replace("```", "").strip()
            classified_data = json.loads(clean_text)

            # 4. Update Supabase (THE FIX IS HERE)
            batch_updates = 0
            for item in classified_data:
                pid = item.get('permit_id')
                tier = item.get('new_tier')
                
                if not pid or not tier:
                    continue
                
                # LOGIC CHANGE: 
                # If AI returns 'Standard', we rename it to 'Ambiguous' 
                # so it gets removed from the fetch queue.
                final_tier = tier
                if tier == 'Standard':
                    final_tier = 'Ambiguous'

                supabase.table('permits')\
                    .update({"complexity_tier": final_tier})\
                    .eq('permit_id', pid)\
                    .execute()
                
                batch_updates += 1
            
            total_processed += batch_updates
            print(f"   ✅ Processed {batch_updates} records. (Total: {total_processed})")
            
            # Sleep to recover tokens
            time.sleep(15) 

        except Exception as e:
            error_str = str(e)
            if "429" in error_str or "Quota exceeded" in error_str:
                print(f"   ⏳ Rate Limit Hit. Sleeping 60s...")
                time.sleep(60)
            else:
                print(f"   !! Batch Error: {e}")
                time.sleep(5)

if __name__ == "__main__":
    run_classification_job()
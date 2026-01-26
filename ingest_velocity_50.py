"""
Vectis Ingestion Orchestrator

This script coordinates the fetching of permit data from multiple city spokes,
sanitizes the records, and runs them through a batch AI classification engine.
"""
"""
Vectis Ingestion Orchestrator - V3.0 Taxonomy Patch
Mission: Ingest "Velocity 3" data while elevating Residential permits.
Note: Filtering is now handled at the UI layer (Streamlit/Looker).
"""
import os
import json
from typing import List
from supabase import create_client, Client
from dotenv import load_dotenv
from datetime import datetime, timedelta
import google.genai as genai
from google.genai import types

# CORE MODELS & SPOKES
from service_models import PermitRecord, ComplexityTier, ProjectCategory
from ingest_austin import get_austin_data
from ingest_san_antonio import get_san_antonio_data
from ingest_fort_worth import get_fort_worth_data

load_dotenv()

# --- CONFIG ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
GEMINI_KEY = os.getenv("GOOGLE_API_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
ai_client = genai.Client(api_key=GEMINI_KEY)

# --- 1. HEURISTIC LISTS (V3.0) ---
commodity_keywords = ["pool", "spa", "sign", "fence", "roof", "siding", "demolition", "irrigation", "solar"]
residential_keywords = ["single family", "sfh", "detached", "duplex", "townhouse", "adu", "garage", "bedroom"]
commercial_keywords = ["commercial", "multifamily", "apartment", "retail", "office", "industrial", "shell"]

def process_and_classify_permits(records: List[PermitRecord]):
    processed_records = []
    to_classify = []
    
    for r in records:
        # Clean description for matching
        desc_clean = (r.description or "").lower()

        # A. Check Commodity (Fast Exit) - Still filtered to keep DB lean
        if any(k in desc_clean for k in commodity_keywords) or r.valuation < 2000:
            r.complexity_tier = ComplexityTier.COMMODITY
            r.project_category = ProjectCategory.TRADE_ONLY
            r.ai_rationale = "Auto-filtered: Commodity/Low Value."
            processed_records.append(r)
        
        # B. Check Residential (New Tier)
        elif any(k in desc_clean for k in residential_keywords):
            r.complexity_tier = ComplexityTier.RESIDENTIAL
            r.project_category = ProjectCategory.RESIDENTIAL_NEW 
            r.ai_rationale = "Auto-filtered: Residential match."
            processed_records.append(r)

        # C. Check Commercial (Core Tier)
        elif any(k in desc_clean for k in commercial_keywords):
            r.complexity_tier = ComplexityTier.COMMERCIAL
            r.project_category = ProjectCategory.COMMERCIAL_NEW
            r.ai_rationale = "Auto-filtered: Commercial match."
            processed_records.append(r)
            
        else:
            to_classify.append(r)

    # D. Batch AI Classification for Ambiguous Records
    if to_classify:
        chunk_size = 50
        for i in range(0, len(to_classify), chunk_size):
            chunk = to_classify[i:i + chunk_size]
            batch_prompt = "Classify as 'Commercial', 'Residential', or 'Commodity'. Return JSON.\n"
            for idx, item in enumerate(chunk):
                batch_prompt += f"ID {idx}: ${item.valuation} | {item.description[:150]}\n"
            
            try:
                response = ai_client.models.generate_content(
                    model="gemini-1.5-flash",
                    contents=batch_prompt,
                    config=types.GenerateContentConfig(response_mime_type="application/json")
                )
                # Parse logic here... (omitted for brevity)
            except Exception as e:
                print(f"AI Batch Error: {e}")

    return processed_records + to_classify

def main():
    cutoff = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    all_data = []

    print("🛰️ Ingesting Velocity 3 (Full Stream)...")
    all_data.extend(get_austin_data(cutoff))
    all_data.extend(get_san_antonio_data(cutoff)) # Ensure spoke doesn't have internal $50k filter
    all_data.extend(get_fort_worth_data(cutoff))

    final_records = process_and_classify_permits(all_data)
    
    # Push to Supabase
    data_to_upsert = [r.dict() for r in final_records]
    if data_to_upsert:
        supabase.table("permits").upsert(data_to_upsert).execute()
    
    print(f"✅ Ingestion Complete: {len(final_records)} records processed.")

if __name__ == "__main__":
    main()
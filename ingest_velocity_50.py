import os
import json
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv
from datetime import datetime, timedelta
import google.genai as genai
from google.genai import types

# 1. CRITICAL: Imports must happen BEFORE functions use these names
# Ensure service_models.py is updated with ProjectCategory as discussed previously
from service_models import PermitRecord, ComplexityTier, ProjectCategory
from ingest_austin import get_austin_data
from ingest_san_antonio import get_san_antonio_data
from ingest_fort_worth import get_fort_worth_data

load_dotenv()

# --- CONFIG ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SOCRATA_TOKEN = os.getenv("SOCRATA_APP_TOKEN")
GEMINI_KEY = os.getenv("GOOGLE_API_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
ai_client = genai.Client(api_key=GEMINI_KEY)

def get_cutoff_date(days_back=90):
    return (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")

# --- SANITIZATION ENGINE (The "Time Travel" Patch) ---
def sanitize_record(record: PermitRecord) -> PermitRecord:
    """
    Fixes dirty data anomalies common in government feeds.
    """
    # 1. Time Travel Check (Issued Date cannot be before Applied Date)
    if record.applied_date and record.issued_date:
        try:
            applied = datetime.strptime(record.applied_date, "%Y-%m-%d")
            issued = datetime.strptime(record.issued_date, "%Y-%m-%d")
            
            if issued < applied:
                # Heuristic: It's usually a cleric swapping fields. Swap them back.
                record.applied_date, record.issued_date = record.issued_date, record.applied_date
        except ValueError:
            pass # Ignore date parse errors, Pydantic handles validation
            
    # 2. Null Description Handling
    if not record.description or record.description.strip() == "":
        record.description = "No Description Provided"
        
    return record

# --- BATCH AI ENGINE ---
def batch_classify_permits(records: list[PermitRecord]):
    if not records: return []
    
    print(f"🧠 Starting Batch Intelligence for {len(records)} records...")
    
    # 1. PRE-FILTER: Aggressive filtering to save tokens and reduce hallucinations
    noise = ["bedroom", "kitchen", "fence", "roofing", "hvac", "deck", "pool", "residential", "single family", "siding", "water heater"]
    to_classify = []
    
    for r in records:
        r = sanitize_record(r) # Apply Time Travel Patch
        
        desc_lower = r.description.lower()
        if any(word in desc_lower for word in noise):
            r.complexity_tier = ComplexityTier.COMMODITY
            r.project_category = ProjectCategory.RESIDENTIAL_ALTERATION
            r.ai_rationale = "Auto-filtered: Residential keywords found."
        else:
            to_classify.append(r)

    print(f"⚡ {len(to_classify)} records require Deep AI classification.")

    # 2. BATCH PROCESSING
    chunk_size = 50
    for i in range(0, len(to_classify), chunk_size):
        chunk = to_classify[i:i + chunk_size]
        print(f"🛰️ Processing AI Chunk {i//chunk_size + 1}...")
        
        # 3. PROMPT ENGINEERING: The "Negative Constraint" Logic
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
        for idx, r in enumerate(chunk):
            batch_prompt += f"ID {idx}: Val=${r.valuation} | Desc: {r.description[:200]}\n"

        try:
            response = ai_client.models.generate_content(
                model="gemini-2.0-flash
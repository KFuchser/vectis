import os
import json
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def run_keyword_sweep():
    print(">> Starting TURBO Keyword Classifier...")
    
    # 1. Fetch Standard Permits
    response = supabase.table('permits')\
        .select('permit_id, description, valuation, city')\
        .eq('complexity_tier', 'Standard')\
        .execute()
    
    permits = response.data
    if not permits:
        print(">> No 'Standard' permits found.")
        return

    print(f">> Analyzing {len(permits)} permits...")
    
    # DEBUG: Show us what we are working with!
    print("\n   --- DATA SAMPLE (First 5) ---")
    for i in range(min(5, len(permits))):
        p = permits[i]
        print(f"   [{p['city']}] Val: ${p['valuation']} | Desc: {p['description']}")
    print("   -----------------------------\n")
    
    updates = []
    
    # 2. Aggressive Logic Rules
    for p in permits:
        desc = (p['description'] or "").lower()
        val = float(p['valuation'] or 0)
        new_tier = None
        
        # --- TIER 1: STRATEGIC (High Value / Commercial) ---
        # Keywords: Construction, Commercial, Multi-family, Industrial
        strategic_words = [
            'commercial', 'multifamily', 'apartments', 'industrial', 
            'warehouse', 'school', 'medical', 'clinic', 'restaurant',
            'retail', 'office', 'shell', 'finish out', 'finish-out', 
            'addition', 'alteration', 'remodel', 'structure', 'bldg',
            'erect', 'construct', 'new', 'ti ', 'tenant', 'fit out'
        ]
        
        # Rule: Any strategic word OR High Valuation (>$50k)
        if val > 50000 or any(w in desc for w in strategic_words):
            new_tier = 'Strategic'

        # --- TIER 2: COMMODITY (Low Value / Residential) ---
        # Keywords: Specific small scope items
        commodity_words = [
            'fence', 'roof', 'sign', 'pool', 'spa', 'solar', 'repair', 
            'driveway', 'patio', 'siding', 'window', 'door', 'hvac', 
            'heater', 'plumbing', 'elec', 'mech', 'irrigation', 'sprinkler',
            'demolition', 'demo', 'res ', 'residential'
        ]
        
        # Rule: Specific keyword AND Low Valuation (<$100k)
        # Note: This OVERRIDES Strategic if it matches (e.g. "Commercial Roof Repair" -> Commodity)
        if any(w in desc for w in commodity_words) and val < 100000:
            new_tier = 'Commodity'
            
        # If we found a match, queue the update
        if new_tier:
            updates.append({
                "permit_id": p['permit_id'],
                "complexity_tier": new_tier
            })

    print(f">> Identified {len(updates)} permits to classify.")

    # 3. Bulk Update
    if not updates:
        print(">> No updates found. Your keywords are still missing the data.")
        return

    batch_size = 100
    for i in range(0, len(updates), batch_size):
        batch = updates[i:i+batch_size]
        for item in batch:
            supabase.table('permits')\
                .update({"complexity_tier": item['complexity_tier']})\
                .eq('permit_id', item['permit_id'])\
                .execute()
        print(f"   ✅ Batch {i//batch_size + 1} complete.")

    print(">> Sweep Complete.")

if __name__ == "__main__":
    run_keyword_sweep()
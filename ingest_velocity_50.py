# --- REFINED BATCH AI ENGINE ---
def batch_classify_permits(records: list[PermitRecord]):
    if not records: return []
    
    print(f"🧠 Starting Batch Intelligence for {len(records)} records...")
    
    noise = ["bedroom", "kitchen", "fence", "roofing", "hvac", "deck", "pool", "residential"]
    to_classify = []
    
    for r in records:
        if any(word in r.description.lower() for word in noise):
            r.complexity_tier = ComplexityTier.COMMODITY
            r.ai_rationale = "Auto-filtered: Residential noise."
        else:
            to_classify.append(r)

    print(f"⚡ {len(to_classify)} records require AI classification.")

    chunk_size = 50
    for i in range(0, len(to_classify), chunk_size):
        chunk = to_classify[i:i + chunk_size]
        print(f"🛰️ Processing AI Chunk {i//chunk_size + 1}...")
        
        batch_prompt = "Classify these permits as 'Strategic' (Commercial) or 'Commodity' (Residential). Return JSON list of {id, tier, reason}.\n\n"
        for idx, r in enumerate(chunk):
            batch_prompt += f"ID {idx}: {r.description[:200]}\n"

        try:
            response = ai_client.models.generate_content(
                model="gemini-2.0-flash",
                contents=batch_prompt,
                config=types.GenerateContentConfig(response_mime_type="application/json")
            )
            
            # Robust JSON Parsing
            results = json.loads(response.text)
            # Handle both list formats and nested "results" keys
            data_list = results.get("results") if isinstance(results, dict) else results

            for res in data_list:
                try:
                    idx = int(res.get("id"))
                    if 0 <= idx < len(chunk):
                        # Ensure we map the string from AI back to our Enum
                        tier_val = res.get("tier", "Commodity").capitalize()
                        chunk[idx].complexity_tier = ComplexityTier(tier_val)
                        chunk[idx].ai_rationale = res.get("reason")
                except Exception as e:
                    continue
        except Exception as e:
            print(f"⚠️ Batch AI Error: {e}")
            
    return records
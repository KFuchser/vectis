import os
import json
from enum import Enum
from typing import Optional
from pydantic import BaseModel  # ✅ model_post_init is NOT imported here
from dotenv import load_dotenv

# --- EXPLICIT 2026 SDK IMPORTS ---
import google.genai as genai  
from google.genai import types

load_dotenv()

# --- AI CONFIGURATION ---
GEMINI_KEY = os.getenv("GOOGLE_API_KEY")
MODEL_ID = "gemini-2.0-flash"

# Initialize Client globally for efficiency
client = genai.Client(api_key=GEMINI_KEY)

class ComplexityTier(str, Enum):
    STRATEGIC = "Strategic"
    COMMODITY = "Commodity"
    UNKNOWN = "Unknown"

class PermitRecord(BaseModel):
    city: str
    permit_id: str
    applied_date: Optional[str] = None
    issued_date: Optional[str] = None
    description: str
    valuation: float = 0.0
    status: str
    # Logic fields calculated by the model
    complexity_tier: ComplexityTier = ComplexityTier.UNKNOWN
    ai_rationale: Optional[str] = None

    def model_post_init(self, __context):
        """
        Runs automatically after Pydantic validation.
        Implements the 'Negative Constraint' logic before calling AI.
        """
        # 1. HARD FILTER (Negative Constraints)
        # Immediately filter out residential noise to save time/cost
        noise_keywords = ["bedroom", "kitchen", "fence", "roofing", "residential", "hvac", "deck", "pool"]
        desc_lower = self.description.lower()
        
        if any(word in desc_lower for word in noise_keywords):
            self.complexity_tier = ComplexityTier.COMMODITY
            self.ai_rationale = "Auto-filtered: Residential noise."
            return

        # 2. AI CLASSIFICATION
        # If it's not obvious noise, we ask Gemini
        try:
            # We add a print here so you can see progress in the GitHub Log
            print(f"DEBUG: AI classifying {self.city} permit {self.permit_id}...")

            prompt = f"""
            Classify this construction permit:
            City: {self.city} | Valuation: ${self.valuation:,.2f}
            Description: "{self.description}"

            Return JSON: {{"tier": "Strategic/Commodity", "reason": "short explanation"}}
            """

            response = client.models.generate_content(
                model=MODEL_ID,
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json"
                )
            )

            res_data = json.loads(response.text)
            self.complexity_tier = ComplexityTier(res_data.get("tier", "Commodity"))
            self.ai_rationale = res_data.get("reason", "AI Classified.")

        except Exception as e:
            # If AI fails, we don't crash the factory
            self.complexity_tier = ComplexityTier.COMMODITY
            self.ai_rationale = f"AI Skip: {str(e)}"
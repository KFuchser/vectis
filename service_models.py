import os
import json
from enum import Enum
from typing import Optional
from pydantic import BaseModel
from dotenv import load_dotenv

# --- UPDATED SDK IMPORTS ---
import google.genai as genai  # ✅ Explicit SDK import
from google.genai import types  # ✅

load_dotenv()

# --- AI CONFIGURATION ---
GEMINI_KEY = os.getenv("GOOGLE_API_KEY")
MODEL_ID = "gemini-2.0-flash"

# Initialize the 2026 Client
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
    complexity_tier: ComplexityTier = ComplexityTier.UNKNOWN
    ai_rationale: Optional[str] = None

    @model_post_init
    def classify_record(self, __context):
        """
        Runs automatically after Pydantic validation.
        Implements 'Negative Constraint' logic before calling AI.
        """
        # 1. HARD FILTER (Negative Constraints)
        noise_keywords = ["bedroom", "kitchen", "fence", "roofing", "residential", "hvac", "deck"]
        desc_lower = self.description.lower()
        
        if any(word in desc_lower for word in noise_keywords):
            self.complexity_tier = ComplexityTier.COMMODITY
            self.ai_rationale = "Automatic filter: Residential noise detected."
            return

        # 2. AI CLASSIFICATION
        try:
            prompt = f"""
            Classify this construction permit:
            City: {self.city} | Valuation: ${self.valuation:,.2f}
            Description: "{self.description}"

            Categorize as 'Strategic' (Commercial/Retail build-outs) or 'Commodity' (Minor repairs/Residential).
            Return ONLY JSON: {{"tier": "Strategic/Commodity", "reason": "text"}}
            """

            response = client.models.generate_content(
                model=MODEL_ID,
                contents=prompt,
                config=types.GenerateContentConfig(response_mime_type="application/json")
            )

            res_data = json.loads(response.text)
            self.complexity_tier = ComplexityTier(res_data.get("tier", "Commodity"))
            self.ai_rationale = res_data.get("reason", "AI Classification complete.")

        except Exception as e:
            # Fixed the unterminated string literal here
            self.complexity_tier = ComplexityTier.COMMODITY
            self.ai_rationale = f"AI Fallback: {str(e)}"
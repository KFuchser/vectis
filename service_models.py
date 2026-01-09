import os
import json
from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field, model_post_init
from dotenv import load_dotenv

# --- UPDATED SDK IMPORTS ---
from google import genai
from google.genai import types

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
    # Logic fields calculated by the model
    complexity_tier: ComplexityTier = ComplexityTier.UNKNOWN
    ai_rationale: Optional[str] = None

    @model_post_init
    def classify_record(self, __context):
        """
        Runs automatically after Pydantic validation.
        Implements the Vectis 'Negative Constraint' logic before calling AI.
        """
        # 1. HARD FILTER (The 'Negative Constraint' check)
        # Prevents spending API budget on obvious residential noise
        noise_keywords = ["bedroom", "kitchen", "fence", "roofing", "residential", "hvac replacement", "deck"]
        desc_lower = self.description.lower()
        
        if any(word in desc_lower for word in noise_keywords):
            self.complexity_tier = ComplexityTier.COMMODITY
            self.ai_rationale = "Automatic filter: Residential noise keyword detected."
            return

        # 2. AI CLASSIFICATION (The 'Awakening')
        try:
            prompt = f"""
            Classify this construction permit for a real estate dashboard.
            City: {self.city}
            Valuation: ${self.valuation:,.2f}
            Description: "{self.description}"

            Categorize as:
            - 'Strategic': Commercial growth, retail build-outs, new businesses.
            - 'Commodity': Minor repairs, standard maintenance, or residential work.

            Return ONLY a valid JSON object with 'tier' and 'reason'.
            """

            # The new Client syntax: client.models.generate_content
            response = client.models.generate_content(
                model=MODEL_ID,
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json"
                )
            )

            # Extract and parse JSON from Gemini response
            res_data = json.loads(response.text)
            self.complexity_tier = ComplexityTier(res_data.get("tier", "Commodity"))
            self.ai_rationale = res_data.get("reason", "AI Classification complete.")

        except Exception as e:
            # Fallback to avoid crashing the whole ingestion script
            print(f"⚠️ AI classification skipped for {self.permit_id}: {e}")
            self.complexity_tier = ComplexityTier.COMMODITY
            self.ai_rationale = "AI
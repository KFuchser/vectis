# --- IMPORTS ---
from datetime import date, datetime
from typing import Optional
from pydantic import BaseModel, Field, field_validator, model_validator

class PermitRecord(BaseModel):
    """
    The Single Source of Truth for a Permit in Vectis.
    Standardizes data from Socrata (Austin) and ArcGIS (Fort Worth/San Antonio).
    """
    
    # --- Core Identity ---
    city: str
    permit_id: str
    
    # --- Velocity Metrics ---
    applied_date: Optional[date] = None
    issued_date: Optional[date] = None
    processing_days: Optional[int] = None # Calculated: Issued - Applied
    
    # --- Context ---
    description: str = "No description provided"
    valuation: float = 0.0
    status: str
    
    # --- Segmentation ---
    # Default is 'Standard'. Logic in validators promotes/demotes it.
    complexity_tier: str = Field(default="Standard", description="Commodity vs. Strategic")

    # 1. VALIDATOR: Clean Dates (Handles Socrata & ArcGIS formats)
    @field_validator('applied_date', 'issued_date', mode='before')
    @classmethod
    def parse_dates(cls, v):
        if not v:
            return None
        if isinstance(v, (date, datetime)):
            return v if isinstance(v, date) else v.date()
        if isinstance(v, str):
            try:
                # Handle ISO format with timestamps (e.g., "2023-10-27T00:00:00")
                return datetime.fromisoformat(v.replace('Z', '')).date()
            except ValueError:
                try:
                    # Fallback for simple YYYY-MM-DD or partial strings
                    return datetime.strptime(v[:10], '%Y-%m-%d').date()
                except:
                    return None
        return v

    # 2. VALIDATOR: The Logic Engine (Velocity + Negative Constraints)
    @model_validator(mode='after')
    def process_logic_gate(self) -> 'PermitRecord':
        # --- A. Calculate Processing Velocity ---
        if self.applied_date and self.issued_date:
            delta = (self.issued_date - self.applied_date).days
            # Filter out negative days (dirty data)
            self.processing_days = delta if delta >= 0 else None
        else:
            self.processing_days = None

        # --- B. Determine Complexity Tier (The "Negative Constraint" Engine) ---
        desc_lower = self.description.lower() if self.description else ""
        val = self.valuation or 0.0
        
        # KEYWORD LISTS
        # These words trigger an automatic "Commodity" classification to save AI costs
        residential_kill_list = [
            'bedroom', 'bathroom', 'kitchen remodel', 'adu', 'dwelling',
            'single family', 'deck', 'fence', 'garage', 'pool', 'residence', 'home'
        ]
        
        commodity_keywords = [
            'roof', 'heater', 'driveway', 'repair', 'siding', 'solar', 'irrigation', 'tent'
        ]

        strategic_keywords = [
            'new construction', 'commercial', 'multifamily', 'apartments', 
            'starbucks', 'retail', 'shell', 'tenant improvement', 'hospital'
        ]

        # LOGIC HIERARCHY
        # Rule 1: Negative Constraint (Residential always wins as a demotion)
        if any(x in desc_lower for x in residential_kill_list):
            self.complexity_tier = "Commodity"
        
        # Rule 2: Commodity Keywords (Small scale maintenance)
        elif any(x in desc_lower for x in commodity_keywords) and val < 50000:
            self.complexity_tier = "Commodity"
        
        # Rule 3: Strategic Promotion (High value OR Commercial keywords)
        elif val > 500000 or any(x in desc_lower for x in strategic_keywords):
            self.complexity_tier = "Strategic"
            
        else:
            # Rule 4: Everything else stays Standard
            self.complexity_tier = "Standard"

        return self
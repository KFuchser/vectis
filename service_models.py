# --- IMPORTS (Do not delete these) ---
from datetime import date, datetime  # <--- This fixes your current error
from typing import Optional          # <--- Fixes 'Optional' errors
from pydantic import BaseModel, Field, field_validator, model_validator

class PermitRecord(BaseModel):
    """
    The Single Source of Truth for a Permit in Vectis.
    Standardizes data from Socrata (Austin) and ArcGIS (Fort Worth).
    """
    
    # --- Core Identity ---
    city: str
    permit_id: str
    
    # --- Velocity Metrics (The "Score") ---
    applied_date: Optional[date] = None
    issued_date: Optional[date] = None
    processing_days: Optional[int] = None # Calculated: Issued - Applied
    
    # --- Context ---
    description: str = "No description provided"
    valuation: float = 0.0
    status: str
    
    # --- Segmentation ---
    # Defaults to 'Standard'. Logic in validators promotes it to 'Strategic' or 'Commodity'
    complexity_tier: str = Field(default="Standard", description="Commodity vs. Strategic")

    # 1. VALIDATOR: Clean Dates
    # Handles messy strings like "2023-10-27T00:00:00" or simple "2023-10-27"
    @field_validator('applied_date', 'issued_date', mode='before')
    def parse_dates(cls, v):
        if not v:
            return None
        if isinstance(v, date): # Already a date object
            return v
        if isinstance(v, str):
            try:
                # Try ISO format first (Socrata default)
                return datetime.fromisoformat(v).date()
            except ValueError:
                try:
                    # Fallback for simple YYYY-MM-DD
                    return datetime.strptime(v, '%Y-%m-%d').date()
                except:
                    return None
        return v

    # 2. VALIDATOR: Calculate Velocity & Tier
    # This runs AFTER the fields are populated
    @model_validator(mode='after')
    def calculate_metrics(self):
        # A. Calculate Processing Velocity (Days)
        if self.applied_date and self.issued_date:
            delta = (self.issued_date - self.applied_date).days
            # Filter out negative days (bad data)
            self.processing_days = delta if delta >= 0 else None
        else:
            self.processing_days = None

        # B. Determine Complexity Tier (Heuristic)
        # We classify based on keywords and valuation to avoid AI costs on small items
        desc_lower = self.description.lower() if self.description else ""
        val = self.valuation or 0.0
        
        # Tier 1: Commodity (Citizen Interest)
        # High volume, low complexity
        commodities = ['fence', 'roof', 'heater', 'driveway', 'repair', 'siding', 'solar', 'irrigation']
        if any(x in desc_lower for x in commodities) and val < 50000:
            self.complexity_tier = "Commodity"
        
        # Tier 2: Strategic (Market Interest)
        # High value or complex scope
        elif val > 500000 or any(x in desc_lower for x in ['new construction', 'commercial', 'multifamily', 'apartments']):
            self.complexity_tier = "Strategic"
            
        # Default remains "Standard"
        return self
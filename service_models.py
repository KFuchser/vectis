import os
from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field
from dotenv import load_dotenv

load_dotenv()

class ComplexityTier(str, Enum):
    STRATEGIC = "Strategic"   # High-value: Retail, Multi-family, Industrial
    COMMODITY = "Commodity"   # Low-value: Single Family, Trade permits (MEP)
    UNKNOWN = "Unknown"

class ProjectCategory(str, Enum):
    # SPECIFICITY FORCES ACCURACY
    RESIDENTIAL_NEW = "Residential - New Construction"
    RESIDENTIAL_ALTERATION = "Residential - Alteration/Addition" 
    COMMERCIAL_NEW = "Commercial - New Construction"
    COMMERCIAL_ALTERATION = "Commercial - Tenant Improvement" 
    INFRASTRUCTURE = "Infrastructure/Public Works"
    TRADE_ONLY = "Trade Only (MEP/Roofing)"

class PermitRecord(BaseModel):
    """
    Lean Data Model (v2.1 Quality Lock - Hotfix)
    Ensures backward compatibility with existing ingestors.
    """
    city: str
    permit_id: str
    applied_date: Optional[str] = None
    issued_date: Optional[str] = None
    description: str
    valuation: float = 0.0
    status: str
    
    # Logic fields (Populated by the Orchestrator)
    complexity_tier: ComplexityTier = ComplexityTier.UNKNOWN
    
    # FIX: Explicitly set default=None so ingestors don't crash
    project_category: Optional[ProjectCategory] = None
    ai_rationale: Optional[str] = Field(default=None, description="Short reason for classification.")
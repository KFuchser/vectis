"""
Defines the core Pydantic data models for the Vectis pipeline.
This includes the central `PermitRecord` model and enumerated types for classification,
ensuring data consistency across all ingestion and processing scripts.
"""
"""
Vectis Service Models
Defines the Pydantic schemas and Enums for the Data Factory.
"""
from enum import Enum
from typing import Optional
from pydantic import BaseModel

class ComplexityTier(str, Enum):
    """
    The 3-Tier Taxonomy.
    """
    COMMODITY = "Commodity"     # Low value, high volume
    RESIDENTIAL = "Residential" # The missing piece causing your crash
    COMMERCIAL = "Commercial"   # High value, strategic
    UNKNOWN = "Unknown"

class ProjectCategory(str, Enum):
    """
    Granular categorization for AI and reporting.
    """
    RESIDENTIAL_NEW = "Residential - New Construction"
    RESIDENTIAL_ALTERATION = "Residential - Alteration"
    COMMERCIAL_NEW = "Commercial - New"
    COMMERCIAL_TI = "Commercial - Tenant Improvement"
    TRADE_ONLY = "Trade Only"
    UNKNOWN = "Unknown"

class PermitRecord(BaseModel):
    """
    The Master Record. All spokes must return this shape.
    """
    permit_id: str
    city: str
    description: Optional[str] = "No Description"
    valuation: float = 0.0
    status: Optional[str] = "Unknown"
    
    # Dates
    applied_date: Optional[str] = None # ISO Format YYYY-MM-DD
    issued_date: Optional[str] = None  # ISO Format YYYY-MM-DD
    
    # Intelligence Fields (Populated by Ingest Orchestrator)
    complexity_tier: ComplexityTier = ComplexityTier.UNKNOWN
    project_category: ProjectCategory = ProjectCategory.UNKNOWN
    ai_rationale: Optional[str] = None
    
    # Location (Optional for MVP)
    latitude: Optional[float] = 0.0
    longitude: Optional[float] = 0.0

    class Config:
        use_enum_values = True # Critical for Supabase JSON compatibility
import os
from enum import Enum
from typing import Optional
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()

class ComplexityTier(str, Enum):
    STRATEGIC = "Strategic"   # High-value commercial/retail
    COMMODITY = "Commodity"   # Residential noise/minor repairs
    UNKNOWN = "Unknown"

class PermitRecord(BaseModel):
    """
    Lean Data Model: 
    Intelligence logic has been moved to the Orchestrator 
    to support High-Velocity Batching.
    """
    city: str
    permit_id: str
    applied_date: Optional[str] = None
    issued_date: Optional[str] = None
    description: str
    valuation: float = 0.0
    status: str
    
    # Logic fields (Filled by Batch Processor in ingest_velocity_50.py)
    complexity_tier: ComplexityTier = ComplexityTier.UNKNOWN
    ai_rationale: Optional[str] = None
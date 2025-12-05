import json
from typing import Dict, Any

def get_mock_vendor_json(service_id: str) -> Dict[str, Any]:

    """
    Simulates fetching raw, vendor-specific JSON data from an external API.
    
    This data intentionally uses messy, inconsistent keys (e.g., 'RecordId', 'CurrentWorkflowStep')
    which our Pydantic ServiceStatus model must clean up using its aliases.
    """

    # 1. Simulate Vendor A (e.g., Accela Format)
    if service_id.startswith("ACC"):
        raw_data = {
            # Use vendor-specific messy keys that test your Pydantic aliases:
            "RecordId": service_id, 
            "RecStatusDesc": "Review Pending", 
            "process_code": "Permitting",
            "Location_Ref": "450 Elm St, 1B",
            "Charge": 1500.00,
            "DateSubmitted": "2025-11-20T10:00:00Z"
        }
        
        # 2. Simulate Vendor B (e.g., Cityworks/Generic Format)
    elif service_id.startswith('CW'):
        raw_data = {
            # Use a different set of keys/casing for Vendor B:
            "AssetID": service_id, 
            "CurrentWorkflowStep": "Awaiting Inspection", 
            "type_of_interaction": "Code Enforcement",
            "geo_address": "450 Elm St",
            "TotalCost": 0.00,
            "EntryDate": "2025-11-24T14:30:00Z"
        }
        
    # 3. Handle Unknown ID
    else:
        raw_data = {
            "Error": "Record Not Found", 
            "AssetID": service_id
        }
        
    return raw_data
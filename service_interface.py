# service_interface.py
import json
from service_models import ServiceStatus
from mock_data import get_mock_vendor_json # Assuming you named the mock file mock_data.py
from typing import Union

def query_system_interface(service_id: str) -> Union[ServiceStatus, str]:
    """
    Acts as the single point of contact for external systems.
    
    This function retrieves the messy vendor data and immediately validates/cleans it
    into the universal ServiceStatus model before passing it to the agent.
    """
    
    # 1. Retrieve the messy vendor data
    vendor_data_dict = get_mock_vendor_json(service_id)
    
    # Check for simple errors from the mock data (e.g., 'Record Not Found')
    if "Error" in vendor_data_dict:
        return f"Error: {vendor_data_dict['Error']} for ID {service_id}"
        
    try:
        # 2. THE ABSTRACTION MAGIC: Validate and Clean
        # Pydantic uses the aliases to find the right vendor keys and enforce types.
        clean_status = ServiceStatus.model_validate(vendor_data_dict)
        
        return clean_status # Returns the perfect, validated Pydantic object
        
    except Exception as e:
        # 3. Handle Pydantic validation failures (e.g., if a fee is negative)
        return f"Validation Error: Data for {service_id} failed schema check. Details: {e}"
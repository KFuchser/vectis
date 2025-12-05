# Quick Test in your main script or a separate test file:
# from service_interface import query_system_interface

# status_accela = query_system_interface("ACC-123")
# status_cityworks = query_system_interface("CW-456")

# print(f"Accela Data Cleaned: {status_accela.service_id}") 
# print(f"Cityworks Data Cleaned: {status_cityworks.service_id}")

# test_interface.py
from service_interface import query_system_interface
from service_models import ServiceStatus 
from typing import Union

# --- Test IDs defined in your mock_data.py ---
ACCELA_ID = "ACC-12345"  # Should trigger the 'Vendor A' (Accela) format
CITYWORKS_ID = "CW-54321" # Should trigger the 'Vendor B' (Cityworks) format

def run_abstraction_test(record_id: str) -> None:
    """Tests the interface with a specific record ID and prints the clean data."""
    print(f"\n--- Testing ID: {record_id} ---")
    
    # Call the abstraction firewall function (Step 2.2)
    result: Union[ServiceStatus, str] = query_system_interface(record_id)
    
    if isinstance(result, ServiceStatus):
        print("✅ SUCCESS: Data was cleaned and validated!")
        print(f"  Vendor Format Used: {record_id.split('-')[0]} (based on ID prefix)")
        
        # This proves the abstraction works—the output keys are clean and universal:
        print("  Clean Output Keys:")
        print(f"    Service ID: {result.service_id}") 
        print(f"    Status: {result.status_detail}")
        print(f"    Process Type: {result.process_type}")
        print(f"    Fee Amount: ${result.fee_amount:.2f}")

    else:
        print(f"❌ FAILURE: Interface returned an error: {result}")


if __name__ == "__main__":
    # Run the tests
    run_abstraction_test(ACCELA_ID)
    run_abstraction_test(CITYWORKS_ID)
    run_abstraction_test("UNKNOWN-999") # Test the error handler
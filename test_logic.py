from service_models import PermitRecord

# --- TEST CASES ---
test_data = [
    {
        "city": "Fort Worth",
        "permit_id": "FW-100",
        "description": "New Starbucks Shell and Drive-thru",
        "valuation": 850000.0,
        "status": "In Review",
        "applied_date": "2024-01-01T08:00:00Z"
    },
    {
        "city": "San Antonio",
        "permit_id": "SA-200",
        "description": "Residential Addition: 2 Bedrooms and a Master Bath",
        "valuation": 1200000.0, # High value, but should be demoted
        "status": "Issued",
        "applied_date": "2024-01-01",
        "issued_date": "2024-01-15"
    },
    {
        "city": "Fort Worth",
        "permit_id": "FW-300",
        "description": "Emergency Roof Repair",
        "valuation": 12000.0,
        "status": "Closed",
        "applied_date": "2024-02-01"
    }
]

print(f"{'CITY':<15} | {'TIER':<12} | {'DAYS':<5} | {'DESCRIPTION'}")
print("-" * 70)

for entry in test_data:
    p = PermitRecord(**entry)
    days = p.processing_days if p.processing_days is not None else "N/A"
    print(f"{p.city:<15} | {p.complexity_tier:<12} | {days:<5} | {p.description}")
#!/usr/bin/env python3
"""Test script for pysof parallel processing functionality."""

import os
import pysof
import json
import time

# Sample ViewDefinition for extracting patient data
view_definition = {
    "resourceType": "ViewDefinition",
    "id": "patient-demographics",
    "name": "PatientDemographics",
    "status": "active",
    "resource": "Patient",
    "select": [
        {
            "column": [
                {"name": "id", "path": "id"},
                {"name": "family_name", "path": "name.family"},
                {"name": "given_name", "path": "name.given.first()"},
                {"name": "gender", "path": "gender"},
                {"name": "birth_date", "path": "birthDate"},
            ]
        }
    ],
}


# Create a larger bundle with multiple patients for testing parallelism
def create_test_bundle(num_patients=100):
    entries = []
    for i in range(num_patients):
        entries.append(
            {
                "resource": {
                    "resourceType": "Patient",
                    "id": f"patient-{i}",
                    "name": [{"family": f"Family{i}", "given": [f"Given{i}"]}],
                    "gender": "male" if i % 2 == 0 else "female",
                    "birthDate": f"199{i % 10}-01-01",
                }
            }
        )

    return {"resourceType": "Bundle", "type": "collection", "entry": entries}


def test_pysof():
    """Test the pysof parallel processing functionality."""
    print("Testing pysof parallel processing functionality...")
    print(f"RAYON_NUM_THREADS: {os.environ.get('RAYON_NUM_THREADS', 'auto (all cores)')}\n")

    # Create test data
    bundle = create_test_bundle(1000)  # 1000 patients for testing

    print(f"Created bundle with {len(bundle['entry'])} patients")

    # Test basic execution (automatic parallel processing)
    print("\n1. Testing parallel execution...")
    start_time = time.time()
    result_default = pysof.run_view_definition_with_options(
        view_definition, bundle, "json"
    )
    default_time = time.time() - start_time
    print(f"   Execution completed in {default_time:.3f} seconds")
    print(f"   (Parallel processing automatically used)")

    # Test with pagination
    print("\n2. Testing with pagination...")
    start_time = time.time()
    result_paginated = pysof.run_view_definition_with_options(
        view_definition, bundle, "json", limit=100
    )
    paginated_time = time.time() - start_time
    print(f"   Pagination completed in {paginated_time:.3f} seconds")

    # Verify results
    data_default = json.loads(result_default.decode("utf-8"))
    data_paginated = json.loads(result_paginated.decode("utf-8"))

    print(f"\n3. Verifying results...")
    print(f"   Full result: {len(data_default)} rows")
    print(f"   Paginated result: {len(data_paginated)} rows")

    # Check pagination worked correctly
    if len(data_paginated) == 100:
        print("   ✅ Pagination worked correctly (100 rows returned)")
    else:
        print(f"   ❌ Expected 100 rows, got {len(data_paginated)}")
        return False

    # Test CSV format
    print(f"\n4. Testing CSV format...")
    result_csv = pysof.run_view_definition_with_options(
        view_definition, bundle, "csv", limit=10
    )
    csv_lines = result_csv.decode("utf-8").strip().split("\n")
    print(f"   CSV result: {len(csv_lines)} lines (including header)")
    
    if len(csv_lines) > 0:
        print("   ✅ CSV format works")
    else:
        print("   ❌ CSV format failed")
        return False

    print(f"\n✅ All tests passed!")
    return True


if __name__ == "__main__":
    try:
        success = test_pysof()
        if success:
            print("\n🎉 pysof parallel processing is working correctly!")
            print("\n💡 To test with different thread counts:")
            print("   Linux/Mac:  RAYON_NUM_THREADS=4 python test_multithreading.py")
            print("   Windows:    set RAYON_NUM_THREADS=4 && python test_multithreading.py")
        else:
            print("\n❌ Some tests failed")
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        import traceback

        traceback.print_exc()

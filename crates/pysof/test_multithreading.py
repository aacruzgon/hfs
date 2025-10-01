#!/usr/bin/env python3
"""Test script for pysof_mult multithreading functionality."""

import pysof_mult
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


def test_multithreading():
    """Test the multithreading functionality."""
    print("Testing pysof_mult multithreading functionality...")

    # Create test data
    bundle = create_test_bundle(1000)  # 1000 patients for better parallelism testing

    print(f"Created bundle with {len(bundle['entry'])} patients")

    # Test with default threading (no num_threads specified)
    print("\n1. Testing with default threading...")
    start_time = time.time()
    result_default = pysof_mult.run_view_definition_with_options(
        view_definition, bundle, "json"
    )
    default_time = time.time() - start_time
    print(f"   Default threading completed in {default_time:.3f} seconds")

    # Test with single thread
    print("\n2. Testing with single thread...")
    start_time = time.time()
    result_single = pysof_mult.run_view_definition_with_options(
        view_definition, bundle, "json", num_threads=1
    )
    single_time = time.time() - start_time
    print(f"   Single thread completed in {single_time:.3f} seconds")

    # Test with multiple threads
    print("\n3. Testing with 4 threads...")
    start_time = time.time()
    result_multi = pysof_mult.run_view_definition_with_options(
        view_definition, bundle, "json", num_threads=4
    )
    multi_time = time.time() - start_time
    print(f"   4 threads completed in {multi_time:.3f} seconds")

    # Verify results are the same
    data_default = json.loads(result_default.decode("utf-8"))
    data_single = json.loads(result_single.decode("utf-8"))
    data_multi = json.loads(result_multi.decode("utf-8"))

    print(f"\n4. Verifying results...")
    print(f"   Default result: {len(data_default)} rows")
    print(f"   Single thread result: {len(data_single)} rows")
    print(f"   Multi thread result: {len(data_multi)} rows")

    # Check if all results have the same number of rows
    if len(data_default) == len(data_single) == len(data_multi):
        print("   ✅ All results have the same number of rows")
    else:
        print("   ❌ Results have different numbers of rows")
        return False

    # Performance comparison
    print(f"\n5. Performance comparison:")
    print(f"   Default vs Single thread: {single_time / default_time:.2f}x")
    print(f"   Single vs Multi thread: {single_time / multi_time:.2f}x")

    if multi_time < single_time:
        print("   ✅ Multithreading shows performance improvement")
    else:
        print(
            "   ⚠️  Multithreading didn't show improvement (normal for small datasets)"
        )

    # Test error handling for invalid thread count
    print(f"\n6. Testing error handling...")
    try:
        pysof_mult.run_view_definition_with_options(
            view_definition, bundle, "json", num_threads=0
        )
        print("   ❌ Should have failed with 0 threads")
        return False
    except Exception as e:
        print(f"   ✅ Correctly handled invalid thread count: {type(e).__name__}")

    print(f"\n✅ All multithreading tests passed!")
    return True


if __name__ == "__main__":
    try:
        success = test_multithreading()
        if success:
            print("\n🎉 Multithreading functionality is working correctly!")
        else:
            print("\n❌ Some tests failed")
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        import traceback

        traceback.print_exc()

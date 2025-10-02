#!/usr/bin/env python3
"""Example demonstrating pysof capabilities."""

import pysof
import json
import time


def main():
    """Demonstrate pysof functionality."""
    print("🧵 pysof Example")
    print("=" * 40)

    # Sample ViewDefinition
    view_definition = {
        "resourceType": "ViewDefinition",
        "id": "patient-example",
        "name": "PatientExample",
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

    # Create sample bundle with multiple patients
    bundle = {
        "resourceType": "Bundle",
        "type": "collection",
        "entry": [
            {
                "resource": {
                    "resourceType": "Patient",
                    "id": f"patient-{i}",
                    "name": [{"family": f"Smith{i}", "given": [f"John{i}"]}],
                    "gender": "male" if i % 2 == 0 else "female",
                    "birthDate": f"198{i % 10}-05-15",
                }
            }
            for i in range(500)  # 500 patients for demonstration
        ],
    }

    print(f"Processing {len(bundle['entry'])} patients...\n")

    # Example 1: Basic execution
    print("1️⃣ Basic execution:")
    start = time.time()
    result = pysof.run_view_definition_with_options(
        view_definition, bundle, "json"
    )
    duration = time.time() - start
    data = json.loads(result.decode("utf-8"))
    print(f"   ⏱️  Time: {duration:.3f}s | Rows: {len(data)}")

    # Example 4: Show sample output
    print(f"\n📋 Sample output (first 3 rows):")
    sample_data = json.loads(result.decode("utf-8"))[:3]
    for i, row in enumerate(sample_data, 1):
        print(f"   {i}. {row}")

    # Example 2: Combined with other options
    print(f"\n🔧 Combined with pagination (limit 10):")
    result = pysof.run_view_definition_with_options(
        view_definition,
        bundle,
        "csv",  # CSV format
        limit=10,
    )
    csv_output = result.decode("utf-8")
    print("   CSV Output:")
    for line in csv_output.strip().split("\n")[:5]:  # Show first 5 lines
        print(f"   {line}")

    print(f"\n✅ Example completed!")
    print(f"\n💡 Tips:")
    print(f"   • Use pagination (limit/page) to control result size")
    print(f"   • Use 'since' parameter to filter by modification date")
    print(f"   • Multiple output formats supported: csv, json, ndjson, parquet")


if __name__ == "__main__":
    main()

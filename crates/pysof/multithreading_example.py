#!/usr/bin/env python3
"""Example demonstrating pysof_mult multithreading capabilities."""

import pysof_mult
import json
import time


def main():
    """Demonstrate multithreading functionality."""
    print("🧵 pysof_mult Multithreading Example")
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

    # Example 1: Default threading (uses system default)
    print("1️⃣ Default threading:")
    start = time.time()
    result = pysof_mult.run_view_definition_with_options(
        view_definition, bundle, "json"
    )
    duration = time.time() - start
    data = json.loads(result.decode("utf-8"))
    print(f"   ⏱️  Time: {duration:.3f}s | Rows: {len(data)}")

    # Example 2: Single thread
    print("\n2️⃣ Single thread:")
    start = time.time()
    result = pysof_mult.run_view_definition_with_options(
        view_definition, bundle, "json", num_threads=1
    )
    duration = time.time() - start
    data = json.loads(result.decode("utf-8"))
    print(f"   ⏱️  Time: {duration:.3f}s | Rows: {len(data)}")

    # Example 3: Multiple threads
    for threads in [2, 4, 8]:
        print(f"\n{threads}️⃣ {threads} threads:")
        start = time.time()
        result = pysof_mult.run_view_definition_with_options(
            view_definition, bundle, "json", num_threads=threads
        )
        duration = time.time() - start
        data = json.loads(result.decode("utf-8"))
        print(f"   ⏱️  Time: {duration:.3f}s | Rows: {len(data)}")

    # Example 4: Show sample output
    print(f"\n📋 Sample output (first 3 rows):")
    sample_data = json.loads(result.decode("utf-8"))[:3]
    for i, row in enumerate(sample_data, 1):
        print(f"   {i}. {row}")

    # Example 5: Combined with other options
    print(f"\n🔧 Combined with pagination (4 threads, limit 10):")
    result = pysof_mult.run_view_definition_with_options(
        view_definition,
        bundle,
        "csv",  # CSV format
        limit=10,
        num_threads=4,
    )
    csv_output = result.decode("utf-8")
    print("   CSV Output:")
    for line in csv_output.strip().split("\n")[:5]:  # Show first 5 lines
        print(f"   {line}")

    print(f"\n✅ Multithreading example completed!")
    print(f"\n💡 Tips:")
    print(f"   • Use more threads for larger datasets")
    print(f"   • Single thread may be faster for small datasets due to overhead")
    print(f"   • Optimal thread count depends on your CPU cores and data size")
    print(f"   • Thread count validation prevents invalid values (e.g., 0 threads)")


if __name__ == "__main__":
    main()

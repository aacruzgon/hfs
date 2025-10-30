# Testing pysof from PyPI

## Manual Testing Required

As requested in the original task, we need to test the pysof package by downloading it from PyPI and running the basic example.

## Test Instructions

### 1. Create Fresh Virtual Environment

```bash
# Create new virtual environment
python -m venv pypi-test-env

# Activate it
# Windows:
pypi-test-env\Scripts\activate
# Linux/Mac:
source pypi-test-env/bin/activate
```

### 2. Install from PyPI

```bash
pip install pysof
```

### 3. Verify Installation

```bash
# Check version
python -c "import pysof; print(f'Version: {pysof.__version__}')"

# Check FHIR versions
python -c "import pysof; print(f'FHIR versions: {pysof.get_supported_fhir_versions()}')"
```

### 4. Run Basic Example

Test with the basic example from the README:

```python
import pysof
import json

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
                {"name": "birth_date", "path": "birthDate"}
            ]
        }
    ]
}

# Sample FHIR Bundle with patient data
bundle = {
    "resourceType": "Bundle",
    "type": "collection", 
    "entry": [
        {
            "resource": {
                "resourceType": "Patient",
                "id": "patient-1",
                "name": [{"family": "Doe", "given": ["John"]}],
                "gender": "male",
                "birthDate": "1990-01-01"
            }
        },
        {
            "resource": {
                "resourceType": "Patient", 
                "id": "patient-2",
                "name": [{"family": "Smith", "given": ["Jane"]}],
                "gender": "female",
                "birthDate": "1985-05-15"
            }
        }
    ]
}

# Transform to different formats
csv_result = pysof.run_view_definition(view_definition, bundle, "csv")
json_result = pysof.run_view_definition(view_definition, bundle, "json")
ndjson_result = pysof.run_view_definition(view_definition, bundle, "ndjson")
parquet_result = pysof.run_view_definition(view_definition, bundle, "parquet")

print("CSV Output:")
print(csv_result.decode('utf-8'))

print("\nJSON Output:")
data = json.loads(json_result.decode('utf-8'))
print(json.dumps(data, indent=2))

print("\nNDJSON Output:")
print(ndjson_result.decode('utf-8'))

print(f"\nParquet Output (binary): {len(parquet_result)} bytes")
```

### 5. Expected Output

The CSV output should show:
```csv
id,family_name,given_name,gender,birth_date
patient-1,Doe,John,male,1990-01-01
patient-2,Smith,Jane,female,1985-05-15
```

The JSON output should show an array of objects with the same data.

### 6. Test Edge Cases

```python
import pysof

# Test validation
try:
    invalid_view = {"invalid": "structure"}
    result = pysof.run_view_definition(invalid_view, bundle, "csv")
except pysof.InvalidViewDefinitionError as e:
    print(f"✅ Validation working: {e}")

# Test unsupported format
try:
    result = pysof.run_view_definition(view_definition, bundle, "xml")
except pysof.UnsupportedContentTypeError as e:
    print(f"✅ Error handling working: {e}")
```

### 7. Check PyPI Page

Visit https://pypi.org/project/pysof/ and verify:

- [ ] Version number is correct (should be 0.1.25 after next release)
- [ ] Project links visible in left sidebar:
  - [ ] Homepage (should point to crates/pysof)
  - [ ] Repository (root repo is OK)
  - [ ] Documentation (should point to crates/pysof)
  - [ ] Bug Tracker
  - [ ] Source (should point to crates/pysof)
- [ ] README renders correctly:
  - [ ] Badges display
  - [ ] Code blocks have syntax highlighting
  - [ ] Tables formatted properly
  - [ ] Emoji display or degrade gracefully
- [ ] Supported platforms listed correctly
- [ ] License shows as MIT

## Issues to Watch For

### Current PyPI Version (0.1.3)
The current version on PyPI is 0.1.3 which:
- Uses old inline dict format for project-urls (may not display links)
- Has older README structure
- Version doesn't match workspace version

### After Next Release (0.1.25)
The improvements in this branch will:
- ✅ Match workspace version
- ✅ Use proper [project.urls] table format
- ✅ Have restructured README with badges
- ✅ Point Homepage and Source to pysof crate specifically

## Clean Up

After testing:
```bash
deactivate
# Remove test environment
# Windows:
rmdir /s /q pypi-test-env
# Linux/Mac:
rm -rf pypi-test-env
```

## Reporting Results

Document your findings:
- Did import work?
- Did the basic example run successfully?
- Were there any errors?
- Are all output formats working?
- Do the project links appear on PyPI?
- Does the README render correctly?

Create an issue at https://github.com/HeliosSoftware/hfs/issues if any problems are found.

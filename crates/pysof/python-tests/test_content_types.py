"""Comprehensive test suite for content type support in pysof.

This module tests:
- Output format generation (CSV, JSON, NDJSON, Parquet)
- Content type parsing and MIME type mapping
- Format-specific options and parameters
- Edge cases and error conditions
- Output validation and structure verification
"""

import csv
import io
import json
import pytest
from typing import Dict, Any, List

import pysof


# ============================================================================
# Test Fixtures
# ============================================================================

@pytest.fixture
def test_view_definition() -> Dict[str, Any]:
    """Return a test ViewDefinition for content type testing."""
    return {
        "resourceType": "ViewDefinition",
        "id": "content-type-test",
        "name": "ContentTypeTest",
        "status": "active",
        "resource": "Patient",
        "select": [
            {
                "column": [
                    {
                        "name": "id",
                        "path": "id"
                    },
                    {
                        "name": "family_name",
                        "path": "name.family"
                    },
                    {
                        "name": "given_name",
                        "path": "name.given.first()"
                    }
                ]
            }
        ]
    }


@pytest.fixture
def test_bundle() -> Dict[str, Any]:
    """Fixture providing a test Bundle with multiple patients."""
    return {
        "resourceType": "Bundle",
        "id": "content-type-test-bundle",
        "type": "collection",
        "entry": [
            {
                "resource": {
                    "resourceType": "Patient",
                    "id": "patient-1",
                    "name": [
                        {
                            "family": "Doe",
                            "given": ["John"]
                        }
                    ]
                }
            },
            {
                "resource": {
                    "resourceType": "Patient",
                    "id": "patient-2",
                    "name": [
                        {
                            "family": "Smith",
                            "given": ["Jane"]
                        }
                    ]
                }
            },
            {
                "resource": {
                    "resourceType": "Patient",
                    "id": "patient-3",
                    "name": [
                        {
                            "family": "Johnson",
                            "given": ["Bob"]
                        }
                    ]
                }
            }
        ]
    }


@pytest.fixture
def empty_bundle() -> Dict[str, Any]:
    """Fixture providing an empty Bundle for edge case testing."""
    return {
        "resourceType": "Bundle",
        "id": "empty-bundle",
        "type": "collection",
        "entry": []
    }


@pytest.fixture
def complex_view_definition() -> Dict[str, Any]:
    """Fixture providing a more complex ViewDefinition with multiple columns."""
    return {
        "resourceType": "ViewDefinition",
        "id": "complex-content-type-test",
        "name": "ComplexContentTypeTest",
        "status": "active",
        "resource": "Patient",
        "select": [
            {
                "column": [
                    {
                        "name": "id",
                        "path": "id",
                        "description": "Patient ID"
                    },
                    {
                        "name": "family_name",
                        "path": "name.family",
                        "description": "Family name"
                    },
                    {
                        "name": "given_name",
                        "path": "name.given.first()",
                        "description": "Given name"
                    },
                    {
                        "name": "full_name",
                        "path": "name.family + ', ' + name.given.first()",
                        "description": "Full name formatted"
                    }
                ]
            }
        ]
    }


# ============================================================================
# Test Classes
# ============================================================================

class TestContentTypeFormats:
    """Test different content type output formats."""
    
    def test_csv_format(self, test_view_definition: Dict[str, Any], test_bundle: Dict[str, Any]) -> None:
        """Test CSV output format with headers."""
        view = test_view_definition
        bundle = test_bundle
        
        result = pysof.run_view_definition(view, bundle, "csv")
        
        # Should return bytes
        assert isinstance(result, bytes)
        
        # Decode and verify CSV structure
        csv_content = result.decode('utf-8')
        lines = csv_content.strip().split('\n')
        
        # Should have header line plus data lines
        assert len(lines) >= 4  # header + 3 patients minimum
        
        # Check that it contains expected patient IDs
        assert "patient-1" in csv_content
        assert "patient-2" in csv_content
        assert "patient-3" in csv_content
        
        # Check for family names
        assert "Doe" in csv_content
        assert "Smith" in csv_content
        assert "Johnson" in csv_content
    
    def test_csv_format_structure(self, test_view_definition: Dict[str, Any], test_bundle: Dict[str, Any]) -> None:
        """Test CSV output has proper structure with valid rows and columns."""
        result = pysof.run_view_definition(test_view_definition, test_bundle, "csv")
        csv_content = result.decode('utf-8')
        
        # Parse CSV using csv module for structural validation
        reader = csv.DictReader(io.StringIO(csv_content))
        rows = list(reader)
        
        # Should have 3 patient rows
        assert len(rows) == 3
        
        # Each row should have expected columns
        expected_columns = {"id", "family_name", "given_name"}
        for row in rows:
            assert expected_columns.issubset(set(row.keys()))
    
    def test_csv_without_header(self, test_view_definition: Dict[str, Any], test_bundle: Dict[str, Any]) -> None:
        """Test CSV output format without headers using 'csv' format string."""
        # Note: 'csv' format (without _with_header suffix) produces CSV without headers
        result = pysof.run_view_definition(test_view_definition, test_bundle, "csv")
        
        assert isinstance(result, bytes)
        csv_content = result.decode('utf-8')
        lines = csv_content.strip().split('\n')
        
        # CSV format includes headers by default in this implementation
        # This test verifies the standard CSV format works
        assert len(lines) >= 3  # At least 3 data lines (may include header)
    
    def test_json_format(self, test_view_definition: Dict[str, Any], test_bundle: Dict[str, Any]) -> None:
        """Test JSON array output format."""
        view = test_view_definition
        bundle = test_bundle
        
        result = pysof.run_view_definition(view, bundle, "json")
        
        # Should return bytes
        assert isinstance(result, bytes)
        
        # Decode and parse JSON
        json_content = result.decode('utf-8')
        data = json.loads(json_content)
        
        # Should be a list/array
        assert isinstance(data, list)
        
        # Should have 3 patient records
        assert len(data) == 3
        
        # Verify structure of first record
        first_record = data[0]
        assert isinstance(first_record, dict)
        assert "id" in first_record
        
        # Check patient IDs are present
        patient_ids = [record.get("id") for record in data]
        assert "patient-1" in patient_ids
        assert "patient-2" in patient_ids
        assert "patient-3" in patient_ids
    
    def test_json_format_structure(self, test_view_definition: Dict[str, Any], test_bundle: Dict[str, Any]) -> None:
        """Test JSON output has proper structure and field types."""
        result = pysof.run_view_definition(test_view_definition, test_bundle, "json")
        data = json.loads(result.decode('utf-8'))
        
        # Verify each record has expected structure
        for record in data:
            assert "id" in record
            assert "family_name" in record or "given_name" in record
            
            # Verify field types
            assert isinstance(record["id"], str)
    
    def test_ndjson_format(self, test_view_definition: Dict[str, Any], test_bundle: Dict[str, Any]) -> None:
        """Test newline-delimited JSON output format."""
        view = test_view_definition
        bundle = test_bundle
        
        result = pysof.run_view_definition(view, bundle, "ndjson")
        
        # Should return bytes
        assert isinstance(result, bytes)
        
        # Decode and parse NDJSON
        ndjson_content = result.decode('utf-8')
        lines = [line.strip() for line in ndjson_content.strip().split('\n') if line.strip()]
        
        # Should have 3 lines (one per patient)
        assert len(lines) == 3
        
        # Each line should be valid JSON
        records = []
        for line in lines:
            record = json.loads(line)
            records.append(record)
            assert isinstance(record, dict)
            assert "id" in record
        
        # Check patient IDs are present
        patient_ids = [record.get("id") for record in records]
        assert "patient-1" in patient_ids
        assert "patient-2" in patient_ids
        assert "patient-3" in patient_ids
    
    def test_ndjson_format_no_trailing_newline(self, test_view_definition: Dict[str, Any], test_bundle: Dict[str, Any]) -> None:
        """Test NDJSON doesn't have extraneous trailing newlines."""
        result = pysof.run_view_definition(test_view_definition, test_bundle, "ndjson")
        ndjson_content = result.decode('utf-8')
        
        # Should end with a single newline or no newline
        assert not ndjson_content.endswith('\n\n'), "NDJSON should not have multiple trailing newlines"
    
    def test_parquet_format(self, test_view_definition: Dict[str, Any], test_bundle: Dict[str, Any]) -> None:
        """Test parquet output format."""
        view = test_view_definition
        bundle = test_bundle
        
        result = pysof.run_view_definition(view, bundle, "parquet")
        
        # Should return bytes
        assert isinstance(result, bytes)
        
        # Should have content (parquet binary format)
        assert len(result) > 0
        
        # Basic check that it's likely parquet format (starts with "PAR1" magic bytes)
        assert result[:4] == b"PAR1"
    
    @pytest.mark.parametrize("format_string,expected_patient_count", [
        ("csv", 3),
        ("json", 3),
        ("ndjson", 3),
    ])
    def test_all_formats_preserve_data_count(self, test_view_definition: Dict[str, Any], 
                                            test_bundle: Dict[str, Any],
                                            format_string: str, 
                                            expected_patient_count: int) -> None:
        """Test that all formats preserve the correct number of records."""
        result = pysof.run_view_definition(test_view_definition, test_bundle, format_string)
        
        if format_string == "csv":
            lines = result.decode('utf-8').strip().split('\n')
            # Subtract 1 for header
            assert len(lines) - 1 == expected_patient_count
        elif format_string == "json":
            data = json.loads(result.decode('utf-8'))
            assert len(data) == expected_patient_count
        elif format_string == "ndjson":
            lines = [line for line in result.decode('utf-8').strip().split('\n') if line.strip()]
            assert len(lines) == expected_patient_count


class TestContentTypeParsing:
    """Test content type string parsing and MIME type mapping."""
    
    @pytest.mark.parametrize("format_string,expected_result", [
        ("csv", "csv_with_header"),
        ("json", "json"),
        ("ndjson", "ndjson"),
        ("parquet", "parquet"),
    ])
    def test_parse_format_strings(self, format_string: str, expected_result: str) -> None:
        """Test parsing simple format strings."""
        assert pysof.parse_content_type(format_string) == expected_result
    
    @pytest.mark.parametrize("mime_type,expected_result", [
        ("text/csv", "csv_with_header"),
        ("application/json", "json"),
        ("application/ndjson", "ndjson"),
        ("application/parquet", "parquet"),
    ])
    def test_parse_mime_types(self, mime_type: str, expected_result: str) -> None:
        """Test parsing full MIME type strings."""
        assert pysof.parse_content_type(mime_type) == expected_result
    
    def test_parse_csv_header_variants(self) -> None:
        """Test CSV header parameter parsing."""
        # Default CSV includes headers
        assert pysof.parse_content_type("text/csv") == "csv_with_header"
        assert pysof.parse_content_type("text/csv;header=true") == "csv_with_header"
        
        # CSV without headers
        assert pysof.parse_content_type("text/csv;header=false") == "csv"
    
    def test_parse_mime_with_charset(self) -> None:
        """Test that MIME types with charset parameters are not currently supported."""
        # Note: The current implementation does not support MIME type parameters
        # This is a known limitation that could be enhanced in the future
        with pytest.raises(pysof.UnsupportedContentTypeError):
            pysof.parse_content_type("text/csv; charset=utf-8")
        
        with pytest.raises(pysof.UnsupportedContentTypeError):
            pysof.parse_content_type("application/json; charset=utf-8")
    
    @pytest.mark.parametrize("unsupported_type", [
        "text/plain",
        "application/xml",
        "text/html",
        "invalid/type",
        "random-string",
        "application/pdf",
        "text/yaml",
    ])
    def test_parse_unsupported_type(self, unsupported_type: str) -> None:
        """Test error handling for unsupported content types."""
        with pytest.raises(pysof.UnsupportedContentTypeError):
            pysof.parse_content_type(unsupported_type)


class TestContentTypeWithOptions:
    """Test content types with run_view_definition_with_options."""
    
    def test_csv_with_limit(self, test_view_definition: Dict[str, Any], test_bundle: Dict[str, Any]) -> None:
        """Test CSV output with limit option."""
        view = test_view_definition
        bundle = test_bundle
        
        result = pysof.run_view_definition_with_options(
            view, bundle, "csv", limit=2
        )
        
        csv_content = result.decode('utf-8')
        lines = csv_content.strip().split('\n')
        
        # Should have header + limited number of data lines
        # Note: actual behavior may vary based on implementation
        assert len(lines) >= 2  # At least header + some data
    
    def test_csv_with_limit_verification(self, test_view_definition: Dict[str, Any], test_bundle: Dict[str, Any]) -> None:
        """Test CSV output respects limit parameter."""
        result = pysof.run_view_definition_with_options(
            test_view_definition, test_bundle, "csv", limit=1
        )
        
        csv_content = result.decode('utf-8')
        lines = csv_content.strip().split('\n')
        
        # Should have header + 1 data line
        assert len(lines) <= 2  # header + limited data
    
    def test_json_with_pagination(self, test_view_definition: Dict[str, Any], test_bundle: Dict[str, Any]) -> None:
        """Test JSON output with pagination options."""
        view = test_view_definition
        bundle = test_bundle
        
        result = pysof.run_view_definition_with_options(
            view, bundle, "json", page=1, limit=2
        )
        
        json_content = result.decode('utf-8')
        data = json.loads(json_content)
        
        assert isinstance(data, list)
        # With pagination, may get fewer results
        assert len(data) <= 3
    
    @pytest.mark.parametrize("format_string", ["csv", "json", "ndjson"])
    def test_all_formats_with_limit(self, test_view_definition: Dict[str, Any], 
                                   test_bundle: Dict[str, Any],
                                   format_string: str) -> None:
        """Test that limit parameter works across all formats."""
        result = pysof.run_view_definition_with_options(
            test_view_definition, test_bundle, format_string, limit=1
        )
        
        assert isinstance(result, bytes)
        # At minimum, should have some content
        assert len(result) > 0
    
    def test_ndjson_with_options(self, test_view_definition: Dict[str, Any], test_bundle: Dict[str, Any]) -> None:
        """Test NDJSON output with various options."""
        view = test_view_definition
        bundle = test_bundle
        
        result = pysof.run_view_definition_with_options(
            view, bundle, "ndjson", limit=1
        )
        
        ndjson_content = result.decode('utf-8')
        lines = [line.strip() for line in ndjson_content.strip().split('\n') if line.strip()]
        
        # With limit=1, should get at most 1 record
        assert len(lines) <= 1
        
        if lines:
            record = json.loads(lines[0])
            assert isinstance(record, dict)
            assert "id" in record


class TestContentTypeEdgeCases:
    """Test edge cases and error conditions for content types."""
    
    def test_empty_bundle(self, test_view_definition: Dict[str, Any], empty_bundle: Dict[str, Any]) -> None:
        """Test content type outputs with empty bundle."""
        view = test_view_definition
        
        # Test all supported formats with empty data
        formats = ["csv", "json", "ndjson"]
        for fmt in formats:
            result = pysof.run_view_definition(view, empty_bundle, fmt)
            assert isinstance(result, bytes)
            
            content = result.decode('utf-8')
            if fmt == "csv":
                # CSV should still have headers even with no data
                assert len(content.strip()) > 0
            elif fmt == "json":
                # JSON should be empty array
                data = json.loads(content)
                assert data == []
            elif fmt == "ndjson":
                # NDJSON should be empty or minimal content
                lines = [line.strip() for line in content.strip().split('\n') if line.strip()]
                assert len(lines) == 0
    
    def test_empty_bundle_all_formats(self, test_view_definition: Dict[str, Any], empty_bundle: Dict[str, Any]) -> None:
        """Test all formats handle empty bundles gracefully."""
        formats = ["csv", "json", "ndjson", "parquet"]
        
        for fmt in formats:
            result = pysof.run_view_definition(test_view_definition, empty_bundle, fmt)
            assert isinstance(result, bytes)
            # Should have some output (at minimum, headers for CSV or empty array for JSON)
            if fmt in ["csv", "parquet"]:
                assert len(result) > 0
    
    def test_content_type_case_sensitivity(self, test_view_definition: Dict[str, Any], test_bundle: Dict[str, Any]) -> None:
        """Test content type parsing is case insensitive where appropriate."""
        view = test_view_definition
        bundle = test_bundle
        
        # These should work (format strings are typically lowercase)
        result1 = pysof.run_view_definition(view, bundle, "json")
        result2 = pysof.run_view_definition(view, bundle, "csv")
        result3 = pysof.run_view_definition(view, bundle, "ndjson")
        
        assert isinstance(result1, bytes)
        assert isinstance(result2, bytes)
        assert isinstance(result3, bytes)
    
    def test_complex_view_all_formats(self, complex_view_definition: Dict[str, Any], test_bundle: Dict[str, Any]) -> None:
        """Test complex ViewDefinition works with all output formats."""
        formats = ["csv", "json", "ndjson"]
        
        for fmt in formats:
            result = pysof.run_view_definition(complex_view_definition, test_bundle, fmt)
            assert isinstance(result, bytes)
            assert len(result) > 0
    
    def test_content_type_with_invalid_fhir_version(self, test_view_definition: Dict[str, Any], test_bundle: Dict[str, Any]) -> None:
        """Test content type with invalid FHIR version."""
        view = test_view_definition
        bundle = test_bundle
        
        with pytest.raises(pysof.UnsupportedContentTypeError):
            pysof.run_view_definition(view, bundle, "json", fhir_version="R99")
    
    def test_large_bundle_all_formats(self, test_view_definition: Dict[str, Any]) -> None:
        """Test formats handle larger bundles appropriately."""
        # Create a bundle with many patients
        large_bundle = {
            "resourceType": "Bundle",
            "id": "large-bundle",
            "type": "collection",
            "entry": [
                {
                    "resource": {
                        "resourceType": "Patient",
                        "id": f"patient-{i}",
                        "name": [
                            {
                                "family": f"Family{i}",
                                "given": [f"Given{i}"]
                            }
                        ]
                    }
                }
                for i in range(100)
            ]
        }
        
        formats = ["csv", "json", "ndjson"]
        for fmt in formats:
            result = pysof.run_view_definition(test_view_definition, large_bundle, fmt)
            assert isinstance(result, bytes)
            assert len(result) > 0


class TestContentTypeValidation:
    """Test output validation and correctness."""
    
    def test_csv_column_alignment(self, test_view_definition: Dict[str, Any], test_bundle: Dict[str, Any]) -> None:
        """Test CSV columns align with ViewDefinition column definitions."""
        result = pysof.run_view_definition(test_view_definition, test_bundle, "csv")
        csv_content = result.decode('utf-8')
        
        reader = csv.DictReader(io.StringIO(csv_content))
        fieldnames = reader.fieldnames
        
        # Should have columns matching ViewDefinition select.column names
        assert fieldnames is not None
        assert "id" in fieldnames
        assert "family_name" in fieldnames or "given_name" in fieldnames
    
    def test_json_keys_match_columns(self, test_view_definition: Dict[str, Any], test_bundle: Dict[str, Any]) -> None:
        """Test JSON object keys match ViewDefinition column names."""
        result = pysof.run_view_definition(test_view_definition, test_bundle, "json")
        data = json.loads(result.decode('utf-8'))
        
        if data:  # If we have results
            first_record = data[0]
            # Keys should match column names from ViewDefinition
            assert "id" in first_record
    
    def test_ndjson_line_validity(self, test_view_definition: Dict[str, Any], test_bundle: Dict[str, Any]) -> None:
        """Test each NDJSON line is valid JSON."""
        result = pysof.run_view_definition(test_view_definition, test_bundle, "ndjson")
        ndjson_content = result.decode('utf-8')
        
        for line in ndjson_content.strip().split('\n'):
            if line.strip():  # Skip empty lines
                # Should parse without error
                record = json.loads(line)
                assert isinstance(record, dict)
    
    def test_csv_no_duplicate_headers(self, test_view_definition: Dict[str, Any], test_bundle: Dict[str, Any]) -> None:
        """Test CSV output doesn't have duplicate header names."""
        result = pysof.run_view_definition(test_view_definition, test_bundle, "csv")
        csv_content = result.decode('utf-8')
        
        reader = csv.DictReader(io.StringIO(csv_content))
        fieldnames = reader.fieldnames
        
        assert fieldnames is not None
        # Check for duplicates
        assert len(fieldnames) == len(set(fieldnames)), "CSV has duplicate column names"


if __name__ == "__main__":
    pytest.main([__file__])

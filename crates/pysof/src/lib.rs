//! PyO3 bindings for helios-sof
//!
//! This module provides Python bindings for the Rust helios-sof library,
//! enabling Python applications to use SQL-on-FHIR ViewDefinition transformations.

use chrono::{DateTime, Utc};
use helios_sof::{
    ChunkConfig, ChunkedResult, ContentType, NdjsonChunkReader, PreparedViewDefinition,
    ProcessingStats, RunOptions, SofBundle, SofError as RustSofError, SofViewDefinition,
    process_ndjson_chunked, run_view_definition, run_view_definition_with_options,
};
use pyo3::exceptions::{PyException, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use std::fs::File;
use std::io::BufReader;

// Custom Python exception types - using different names to avoid conflicts
pyo3::create_exception!(
    pysof,
    PySofError,
    PyException,
    "Base exception for all pysof errors"
);
pyo3::create_exception!(
    pysof,
    PyInvalidViewDefinitionError,
    PySofError,
    "ViewDefinition validation errors"
);
pyo3::create_exception!(
    pysof,
    PyFhirPathError,
    PySofError,
    "FHIRPath expression evaluation errors"
);
pyo3::create_exception!(
    pysof,
    PySerializationError,
    PySofError,
    "JSON/data serialization errors"
);
pyo3::create_exception!(
    pysof,
    PyUnsupportedContentTypeError,
    PySofError,
    "Unsupported output format errors"
);
pyo3::create_exception!(pysof, PyCsvError, PySofError, "CSV generation errors");
pyo3::create_exception!(pysof, PyIoError, PySofError, "File/IO related errors");
// New source-related error types
pyo3::create_exception!(
    pysof,
    PyInvalidSourceError,
    PySofError,
    "Invalid source parameter value"
);
pyo3::create_exception!(pysof, PySourceNotFoundError, PySofError, "Source not found");
pyo3::create_exception!(
    pysof,
    PySourceFetchError,
    PySofError,
    "Failed to fetch from source"
);
pyo3::create_exception!(
    pysof,
    PySourceReadError,
    PySofError,
    "Failed to read from source"
);
pyo3::create_exception!(
    pysof,
    PyInvalidSourceContentError,
    PySofError,
    "Invalid content in source"
);
pyo3::create_exception!(
    pysof,
    PyUnsupportedSourceProtocolError,
    PySofError,
    "Unsupported source protocol"
);

/// Convert Rust SofError to appropriate Python exception
#[allow(unreachable_patterns)]
fn rust_sof_error_to_py_err(err: RustSofError) -> PyErr {
    match err {
        RustSofError::InvalidViewDefinition(msg) => PyInvalidViewDefinitionError::new_err(msg),
        RustSofError::FhirPathError(msg) => PyFhirPathError::new_err(msg),
        RustSofError::SerializationError(err) => PySerializationError::new_err(err.to_string()),
        RustSofError::UnsupportedContentType(msg) => PyUnsupportedContentTypeError::new_err(msg),
        RustSofError::CsvError(err) => PyCsvError::new_err(err.to_string()),
        RustSofError::IoError(err) => PyIoError::new_err(err.to_string()),
        RustSofError::CsvWriterError(msg) => PyCsvError::new_err(msg),
        RustSofError::InvalidSource(msg) => PyInvalidSourceError::new_err(msg),
        RustSofError::SourceNotFound(msg) => PySourceNotFoundError::new_err(msg),
        RustSofError::SourceFetchError(msg) => PySourceFetchError::new_err(msg),
        RustSofError::SourceReadError(msg) => PySourceReadError::new_err(msg),
        RustSofError::InvalidSourceContent(msg) => PyInvalidSourceContentError::new_err(msg),
        RustSofError::UnsupportedSourceProtocol(msg) => {
            PyUnsupportedSourceProtocolError::new_err(msg)
        }
        // Catch-all for any future error variants
        _ => PySofError::new_err(format!("Unhandled SofError: {}", err)),
    }
}

/// Convert serde_json::Error to Python exception
fn json_error_to_py_err(err: serde_json::Error) -> PyErr {
    PySerializationError::new_err(err.to_string())
}

/// Transform FHIR Bundle data using a ViewDefinition.
///
/// Args:
///     view_definition (dict): ViewDefinition resource as a Python dictionary
///     bundle (dict): FHIR Bundle resource as a Python dictionary  
///     format (str): Output format ("csv", "csv_with_header", "json", "ndjson", "parquet")
///     fhir_version (str, optional): FHIR version to use ("R4", "R4B", "R5", "R6"). Defaults to "R4"
///
/// Returns:
///     bytes: Transformed data in the requested format
///
/// Raises:
///     InvalidViewDefinitionError: ViewDefinition structure is invalid
///     FhirPathError: FHIRPath expression evaluation failed
///     SerializationError: JSON parsing/serialization failed
///     UnsupportedContentTypeError: Unsupported output format
///     CsvError: CSV generation failed
///     IoError: I/O operation failed
#[pyfunction]
#[pyo3(signature = (view_definition, bundle, format, fhir_version = "R4"))]
fn py_run_view_definition(
    py: Python<'_>,
    view_definition: &Bound<'_, PyAny>,
    bundle: &Bound<'_, PyAny>,
    format: &str,
    fhir_version: &str,
) -> PyResult<Py<PyBytes>> {
    // Parse content type
    let content_type = ContentType::from_string(format).map_err(rust_sof_error_to_py_err)?;

    // Parse ViewDefinition and Bundle based on FHIR version
    let view_def_json: serde_json::Value = pythonize::depythonize(view_definition)?;
    let bundle_json: serde_json::Value = pythonize::depythonize(bundle)?;

    let parsed: PyResult<(SofViewDefinition, SofBundle)> = match fhir_version {
        #[cfg(feature = "R4")]
        "R4" => {
            let view_def: helios_fhir::r4::ViewDefinition =
                serde_json::from_value(view_def_json).map_err(json_error_to_py_err)?;
            let bundle: helios_fhir::r4::Bundle =
                serde_json::from_value(bundle_json).map_err(json_error_to_py_err)?;
            Ok((SofViewDefinition::R4(view_def), SofBundle::R4(bundle)))
        }
        #[cfg(feature = "R4B")]
        "R4B" => {
            let view_def: helios_fhir::r4b::ViewDefinition =
                serde_json::from_value(view_def_json).map_err(json_error_to_py_err)?;
            let bundle: helios_fhir::r4b::Bundle =
                serde_json::from_value(bundle_json).map_err(json_error_to_py_err)?;
            Ok((SofViewDefinition::R4B(view_def), SofBundle::R4B(bundle)))
        }
        #[cfg(feature = "R5")]
        "R5" => {
            let view_def: helios_fhir::r5::ViewDefinition =
                serde_json::from_value(view_def_json).map_err(json_error_to_py_err)?;
            let bundle: helios_fhir::r5::Bundle =
                serde_json::from_value(bundle_json).map_err(json_error_to_py_err)?;
            Ok((SofViewDefinition::R5(view_def), SofBundle::R5(bundle)))
        }
        #[cfg(feature = "R6")]
        "R6" => {
            let view_def: helios_fhir::r6::ViewDefinition =
                serde_json::from_value(view_def_json).map_err(json_error_to_py_err)?;
            let bundle: helios_fhir::r6::Bundle =
                serde_json::from_value(bundle_json).map_err(json_error_to_py_err)?;
            Ok((SofViewDefinition::R6(view_def), SofBundle::R6(bundle)))
        }
        _ => Err(PyUnsupportedContentTypeError::new_err(format!(
            "Unsupported FHIR version: {}",
            fhir_version
        ))),
    };

    let (sof_view_def, sof_bundle) = parsed?;

    // Execute transformation - release GIL for parallel/long work
    let result = py
        .detach(|| run_view_definition(sof_view_def, sof_bundle, content_type))
        .map_err(rust_sof_error_to_py_err)?;

    Ok(PyBytes::new(py, &result).into())
}

/// Transform FHIR Bundle data using a ViewDefinition with additional options.
///
/// Args:
///     view_definition (dict): ViewDefinition resource as a Python dictionary
///     bundle (dict): FHIR Bundle resource as a Python dictionary
///     format (str): Output format ("csv", "csv_with_header", "json", "ndjson", "parquet")
///     since (str, optional): Filter resources modified after this ISO8601 datetime
///     limit (int, optional): Limit the number of results returned
///     page (int, optional): Page number for pagination (1-based)
///     fhir_version (str, optional): FHIR version to use ("R4", "R4B", "R5", "R6"). Defaults to "R4"
///
/// Returns:
///     bytes: Transformed data in the requested format
///
/// Raises:
///     InvalidViewDefinitionError: ViewDefinition structure is invalid
///     FhirPathError: FHIRPath expression evaluation failed
///     SerializationError: JSON parsing/serialization failed
///     UnsupportedContentTypeError: Unsupported output format
///     CsvError: CSV generation failed
///     IoError: I/O operation failed
#[pyfunction]
#[pyo3(signature = (view_definition, bundle, format, *, since = None, limit = None, page = None, fhir_version = "R4"))]
#[allow(clippy::too_many_arguments)]
fn py_run_view_definition_with_options(
    py: Python<'_>,
    view_definition: &Bound<'_, PyAny>,
    bundle: &Bound<'_, PyAny>,
    format: &str,
    since: Option<&str>,
    limit: Option<usize>,
    page: Option<usize>,
    fhir_version: &str,
) -> PyResult<Py<PyBytes>> {
    // Parse content type
    let content_type = ContentType::from_string(format).map_err(rust_sof_error_to_py_err)?;

    // Parse ViewDefinition and Bundle based on FHIR version
    let view_def_json: serde_json::Value = pythonize::depythonize(view_definition)?;
    let bundle_json: serde_json::Value = pythonize::depythonize(bundle)?;

    let (sof_view_def, sof_bundle) = match fhir_version {
        #[cfg(feature = "R4")]
        "R4" => {
            let view_def: helios_fhir::r4::ViewDefinition =
                serde_json::from_value(view_def_json).map_err(json_error_to_py_err)?;
            let bundle: helios_fhir::r4::Bundle =
                serde_json::from_value(bundle_json).map_err(json_error_to_py_err)?;
            Ok((SofViewDefinition::R4(view_def), SofBundle::R4(bundle)))
        }
        #[cfg(feature = "R4B")]
        "R4B" => {
            let view_def: helios_fhir::r4b::ViewDefinition =
                serde_json::from_value(view_def_json).map_err(json_error_to_py_err)?;
            let bundle: helios_fhir::r4b::Bundle =
                serde_json::from_value(bundle_json).map_err(json_error_to_py_err)?;
            Ok((SofViewDefinition::R4B(view_def), SofBundle::R4B(bundle)))
        }
        #[cfg(feature = "R5")]
        "R5" => {
            let view_def: helios_fhir::r5::ViewDefinition =
                serde_json::from_value(view_def_json).map_err(json_error_to_py_err)?;
            let bundle: helios_fhir::r5::Bundle =
                serde_json::from_value(bundle_json).map_err(json_error_to_py_err)?;
            Ok((SofViewDefinition::R5(view_def), SofBundle::R5(bundle)))
        }
        #[cfg(feature = "R6")]
        "R6" => {
            let view_def: helios_fhir::r6::ViewDefinition =
                serde_json::from_value(view_def_json).map_err(json_error_to_py_err)?;
            let bundle: helios_fhir::r6::Bundle =
                serde_json::from_value(bundle_json).map_err(json_error_to_py_err)?;
            Ok((SofViewDefinition::R6(view_def), SofBundle::R6(bundle)))
        }
        _ => Err(PyUnsupportedContentTypeError::new_err(format!(
            "Unsupported FHIR version: {}",
            fhir_version
        ))),
    }?;

    // Parse options
    let mut options = RunOptions::default();

    if let Some(since_str) = since {
        options.since = Some(
            since_str
                .parse::<DateTime<Utc>>()
                .map_err(|e| PyValueError::new_err(format!("Invalid 'since' datetime: {}", e)))?,
        );
    }

    options.limit = limit;
    options.page = page;

    // Execute transformation - release GIL for parallel/long work
    let result = py
        .detach(|| {
            run_view_definition_with_options(sof_view_def, sof_bundle, content_type, options)
        })
        .map_err(rust_sof_error_to_py_err)?;

    Ok(PyBytes::new(py, &result).into())
}

/// Validate a ViewDefinition structure without executing it.
///
/// Args:
///     view_definition (dict): ViewDefinition resource as a Python dictionary
///     fhir_version (str, optional): FHIR version to use ("R4", "R4B", "R5", "R6"). Defaults to "R4"
///
/// Returns:
///     bool: True if valid
///
/// Raises:
///     InvalidViewDefinitionError: ViewDefinition structure is invalid
///     SerializationError: JSON parsing failed
#[pyfunction]
#[pyo3(signature = (view_definition, fhir_version = "R4"))]
fn py_validate_view_definition(
    view_definition: &Bound<'_, PyAny>,
    fhir_version: &str,
) -> PyResult<bool> {
    let view_def_json: serde_json::Value = pythonize::depythonize(view_definition)?;

    // Try to parse ViewDefinition for the specified FHIR version
    match fhir_version {
        #[cfg(feature = "R4")]
        "R4" => {
            let _view_def: helios_fhir::r4::ViewDefinition =
                serde_json::from_value(view_def_json).map_err(json_error_to_py_err)?;
            Ok(true)
        }
        #[cfg(feature = "R4B")]
        "R4B" => {
            let _view_def: helios_fhir::r4b::ViewDefinition =
                serde_json::from_value(view_def_json).map_err(json_error_to_py_err)?;
            Ok(true)
        }
        #[cfg(feature = "R5")]
        "R5" => {
            let _view_def: helios_fhir::r5::ViewDefinition =
                serde_json::from_value(view_def_json).map_err(json_error_to_py_err)?;
            Ok(true)
        }
        #[cfg(feature = "R6")]
        "R6" => {
            let _view_def: helios_fhir::r6::ViewDefinition =
                serde_json::from_value(view_def_json).map_err(json_error_to_py_err)?;
            Ok(true)
        }
        _ => Err(PyUnsupportedContentTypeError::new_err(format!(
            "Unsupported FHIR version: {}",
            fhir_version
        ))),
    }
}

/// Validate a Bundle structure without executing transformations.
///
/// Args:
///     bundle (dict): FHIR Bundle resource as a Python dictionary
///     fhir_version (str, optional): FHIR version to use ("R4", "R4B", "R5", "R6"). Defaults to "R4"
///
/// Returns:
///     bool: True if valid
///
/// Raises:
///     SerializationError: JSON parsing failed
#[pyfunction]
#[pyo3(signature = (bundle, fhir_version = "R4"))]
fn py_validate_bundle(bundle: &Bound<'_, PyAny>, fhir_version: &str) -> PyResult<bool> {
    let bundle_json: serde_json::Value = pythonize::depythonize(bundle)?;

    // Try to parse Bundle for the specified FHIR version
    match fhir_version {
        #[cfg(feature = "R4")]
        "R4" => {
            let _bundle: helios_fhir::r4::Bundle =
                serde_json::from_value(bundle_json).map_err(json_error_to_py_err)?;
            Ok(true)
        }
        #[cfg(feature = "R4B")]
        "R4B" => {
            let _bundle: helios_fhir::r4b::Bundle =
                serde_json::from_value(bundle_json).map_err(json_error_to_py_err)?;
            Ok(true)
        }
        #[cfg(feature = "R5")]
        "R5" => {
            let _bundle: helios_fhir::r5::Bundle =
                serde_json::from_value(bundle_json).map_err(json_error_to_py_err)?;
            Ok(true)
        }
        #[cfg(feature = "R6")]
        "R6" => {
            let _bundle: helios_fhir::r6::Bundle =
                serde_json::from_value(bundle_json).map_err(json_error_to_py_err)?;
            Ok(true)
        }
        _ => Err(PyUnsupportedContentTypeError::new_err(format!(
            "Unsupported FHIR version: {}",
            fhir_version
        ))),
    }
}

/// Parse MIME type string to format identifier.
///
/// Args:
///     mime_type (str): MIME type string (e.g., "text/csv", "application/json")
///
/// Returns:
///     str: Format identifier suitable for use with run_view_definition
///
/// Raises:
///     UnsupportedContentTypeError: Unknown or unsupported MIME type
#[pyfunction]
fn py_parse_content_type(mime_type: &str) -> PyResult<String> {
    let content_type = ContentType::from_string(mime_type).map_err(rust_sof_error_to_py_err)?;

    let format_str = match content_type {
        ContentType::Csv => "csv",
        ContentType::CsvWithHeader => "csv_with_header",
        ContentType::Json => "json",
        ContentType::NdJson => "ndjson",
        ContentType::Parquet => "parquet",
    };

    Ok(format_str.to_string())
}

/// Get list of supported FHIR versions compiled into this build.
///
/// Returns:
///     List[str]: List of supported FHIR version strings
#[pyfunction]
#[allow(clippy::vec_init_then_push)]
fn py_get_supported_fhir_versions() -> PyResult<Vec<String>> {
    let mut versions = Vec::new();

    #[cfg(feature = "R4")]
    versions.push("R4".to_string());

    #[cfg(feature = "R4B")]
    versions.push("R4B".to_string());

    #[cfg(feature = "R5")]
    versions.push("R5".to_string());

    #[cfg(feature = "R6")]
    versions.push("R6".to_string());

    Ok(versions)
}

/// Internal struct to hold the chunk iterator state.
/// We use Box<dyn Iterator> to avoid lifetime issues with PyO3.
struct ChunkedIteratorInner {
    reader: NdjsonChunkReader<BufReader<File>>,
    prepared_vd: PreparedViewDefinition,
}

impl ChunkedIteratorInner {
    fn next_chunk(&mut self) -> Option<Result<ChunkedResult, RustSofError>> {
        self.reader.next().map(|chunk_result| {
            chunk_result.and_then(|chunk| self.prepared_vd.process_chunk(chunk))
        })
    }
}

/// Iterator for processing NDJSON files in chunks.
///
/// This class provides a Python iterator interface for processing large NDJSON files
/// containing FHIR resources. Instead of loading the entire file into memory, it
/// processes resources in configurable chunks, yielding results incrementally.
///
/// Args:
///     view_definition (dict): ViewDefinition resource as a Python dictionary
///     input_path (str): Path to the NDJSON file containing FHIR resources
///     chunk_size (int, optional): Number of resources per chunk. Defaults to 1000.
///     skip_invalid (bool, optional): Skip invalid JSON lines. Defaults to False.
///     fhir_version (str, optional): FHIR version ("R4", "R4B", "R5", "R6"). Defaults to "R4".
///
/// Yields:
///     dict: A dictionary containing:
///         - "columns": List of column names
///         - "rows": List of row values (each row is a list of values)
///         - "chunk_index": Zero-based index of this chunk
///         - "is_last": True if this is the final chunk
///
/// Example:
///     >>> import pysof
///     >>> view_def = {"resourceType": "ViewDefinition", "resource": "Patient", ...}
///     >>> for chunk in pysof.ChunkedProcessor(view_def, "patients.ndjson"):
///     ...     for row in chunk["rows"]:
///     ...         process_row(row)
#[pyclass]
struct ChunkedProcessor {
    inner: Option<ChunkedIteratorInner>,
    columns: Option<Vec<String>>,
}

#[pymethods]
impl ChunkedProcessor {
    #[new]
    #[pyo3(signature = (view_definition, input_path, *, chunk_size=1000, skip_invalid=false, fhir_version="R4"))]
    fn new(
        view_definition: &Bound<'_, PyAny>,
        input_path: &str,
        chunk_size: usize,
        skip_invalid: bool,
        fhir_version: &str,
    ) -> PyResult<Self> {
        // Parse ViewDefinition based on FHIR version
        let view_def_json: serde_json::Value = pythonize::depythonize(view_definition)?;

        let sof_view_def: SofViewDefinition = match fhir_version {
            #[cfg(feature = "R4")]
            "R4" => {
                let view_def: helios_fhir::r4::ViewDefinition =
                    serde_json::from_value(view_def_json).map_err(json_error_to_py_err)?;
                SofViewDefinition::R4(view_def)
            }
            #[cfg(feature = "R4B")]
            "R4B" => {
                let view_def: helios_fhir::r4b::ViewDefinition =
                    serde_json::from_value(view_def_json).map_err(json_error_to_py_err)?;
                SofViewDefinition::R4B(view_def)
            }
            #[cfg(feature = "R5")]
            "R5" => {
                let view_def: helios_fhir::r5::ViewDefinition =
                    serde_json::from_value(view_def_json).map_err(json_error_to_py_err)?;
                SofViewDefinition::R5(view_def)
            }
            #[cfg(feature = "R6")]
            "R6" => {
                let view_def: helios_fhir::r6::ViewDefinition =
                    serde_json::from_value(view_def_json).map_err(json_error_to_py_err)?;
                SofViewDefinition::R6(view_def)
            }
            _ => {
                return Err(PyUnsupportedContentTypeError::new_err(format!(
                    "Unsupported FHIR version: {}",
                    fhir_version
                )));
            }
        };

        // Open the file
        let file = File::open(input_path).map_err(|e| PyIoError::new_err(e.to_string()))?;
        let reader = BufReader::new(file);

        // Create config
        let config = ChunkConfig {
            chunk_size,
            skip_invalid_lines: skip_invalid,
        };

        // Create prepared ViewDefinition
        let prepared_vd =
            PreparedViewDefinition::new(sof_view_def).map_err(rust_sof_error_to_py_err)?;

        // Get column names
        let columns = Some(prepared_vd.columns().to_vec());

        // Create chunk reader with resource type filter
        let resource_type = Some(prepared_vd.target_resource_type().to_string());
        let chunk_reader =
            NdjsonChunkReader::new(reader, config).with_resource_type_filter(resource_type);

        Ok(Self {
            inner: Some(ChunkedIteratorInner {
                reader: chunk_reader,
                prepared_vd,
            }),
            columns,
        })
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let inner = match &mut self.inner {
            Some(inner) => inner,
            None => return Ok(None),
        };

        // Release GIL during chunk processing
        let result = py.detach(|| inner.next_chunk());

        match result {
            Some(Ok(chunk)) => {
                // Convert ChunkedResult to Python dict
                let dict = pyo3::types::PyDict::new(py);

                // Add columns
                dict.set_item("columns", &chunk.columns)?;

                // Convert rows - each row is a list of values
                let rows: Vec<Py<PyAny>> = chunk
                    .rows
                    .iter()
                    .map(|row| {
                        let values: Vec<Py<PyAny>> = row
                            .values
                            .iter()
                            .map(|v| match v {
                                Some(val) => pythonize::pythonize(py, val)
                                    .map(|b| b.into())
                                    .unwrap_or_else(|_| py.None()),
                                None => py.None(),
                            })
                            .collect();
                        pyo3::types::PyList::new(py, values).unwrap().into()
                    })
                    .collect();
                dict.set_item("rows", pyo3::types::PyList::new(py, rows)?)?;

                dict.set_item("chunk_index", chunk.chunk_index)?;
                dict.set_item("is_last", chunk.is_last)?;

                Ok(Some(dict.into()))
            }
            Some(Err(e)) => Err(rust_sof_error_to_py_err(e)),
            None => {
                // Iteration complete
                self.inner = None;
                Ok(None)
            }
        }
    }

    /// Get the column names for this ViewDefinition.
    ///
    /// Returns:
    ///     List[str]: Column names in order
    #[getter]
    fn columns(&self) -> Option<Vec<String>> {
        self.columns.clone()
    }
}

/// Convert ProcessingStats to a Python dictionary
fn stats_to_pydict(py: Python<'_>, stats: &ProcessingStats) -> PyResult<Py<PyAny>> {
    let dict = pyo3::types::PyDict::new(py);
    dict.set_item("total_lines_read", stats.total_lines_read)?;
    dict.set_item("resources_processed", stats.resources_processed)?;
    dict.set_item("output_rows", stats.output_rows)?;
    dict.set_item("skipped_lines", stats.skipped_lines)?;
    dict.set_item("chunks_processed", stats.chunks_processed)?;
    Ok(dict.into())
}

/// Process an NDJSON file and write output to a file.
///
/// This function processes an NDJSON file containing FHIR resources using a ViewDefinition
/// and writes the output directly to a file. It uses chunked processing for memory efficiency.
///
/// Args:
///     view_definition (dict): ViewDefinition resource as a Python dictionary
///     input_path (str): Path to the NDJSON file containing FHIR resources
///     output_path (str): Path to write the output file
///     format (str): Output format ("csv", "csv_with_header", "ndjson")
///     chunk_size (int, optional): Number of resources per chunk. Defaults to 1000.
///     skip_invalid (bool, optional): Skip invalid JSON lines. Defaults to False.
///     fhir_version (str, optional): FHIR version ("R4", "R4B", "R5", "R6"). Defaults to "R4".
///
/// Returns:
///     dict: Processing statistics containing:
///         - "total_lines_read": Total lines read from input
///         - "resources_processed": Number of FHIR resources processed
///         - "output_rows": Number of output rows written
///         - "skipped_lines": Number of invalid lines skipped
///         - "chunks_processed": Number of chunks processed
///
/// Raises:
///     InvalidViewDefinitionError: ViewDefinition structure is invalid
///     FhirPathError: FHIRPath expression evaluation failed
///     IoError: File operation failed
///     UnsupportedContentTypeError: Unsupported output format (e.g., Parquet not supported for streaming)
#[pyfunction]
#[pyo3(signature = (view_definition, input_path, output_path, format, *, chunk_size=1000, skip_invalid=false, fhir_version="R4"))]
#[allow(clippy::too_many_arguments)]
fn py_process_ndjson_to_file(
    py: Python<'_>,
    view_definition: &Bound<'_, PyAny>,
    input_path: &str,
    output_path: &str,
    format: &str,
    chunk_size: usize,
    skip_invalid: bool,
    fhir_version: &str,
) -> PyResult<Py<PyAny>> {
    // Parse content type
    let content_type = ContentType::from_string(format).map_err(rust_sof_error_to_py_err)?;

    // Parse ViewDefinition based on FHIR version
    let view_def_json: serde_json::Value = pythonize::depythonize(view_definition)?;

    let sof_view_def: SofViewDefinition = match fhir_version {
        #[cfg(feature = "R4")]
        "R4" => {
            let view_def: helios_fhir::r4::ViewDefinition =
                serde_json::from_value(view_def_json).map_err(json_error_to_py_err)?;
            SofViewDefinition::R4(view_def)
        }
        #[cfg(feature = "R4B")]
        "R4B" => {
            let view_def: helios_fhir::r4b::ViewDefinition =
                serde_json::from_value(view_def_json).map_err(json_error_to_py_err)?;
            SofViewDefinition::R4B(view_def)
        }
        #[cfg(feature = "R5")]
        "R5" => {
            let view_def: helios_fhir::r5::ViewDefinition =
                serde_json::from_value(view_def_json).map_err(json_error_to_py_err)?;
            SofViewDefinition::R5(view_def)
        }
        #[cfg(feature = "R6")]
        "R6" => {
            let view_def: helios_fhir::r6::ViewDefinition =
                serde_json::from_value(view_def_json).map_err(json_error_to_py_err)?;
            SofViewDefinition::R6(view_def)
        }
        _ => {
            return Err(PyUnsupportedContentTypeError::new_err(format!(
                "Unsupported FHIR version: {}",
                fhir_version
            )));
        }
    };

    // Open files
    let input_file = File::open(input_path).map_err(|e| PyIoError::new_err(e.to_string()))?;
    let input_reader = BufReader::new(input_file);

    let output_file = File::create(output_path).map_err(|e| PyIoError::new_err(e.to_string()))?;
    let output_writer = std::io::BufWriter::new(output_file);

    // Create config
    let config = ChunkConfig {
        chunk_size,
        skip_invalid_lines: skip_invalid,
    };

    // Process - release GIL during processing
    let stats = py
        .detach(|| {
            process_ndjson_chunked(
                sof_view_def,
                input_reader,
                output_writer,
                content_type,
                config,
            )
        })
        .map_err(rust_sof_error_to_py_err)?;

    stats_to_pydict(py, &stats)
}

/// Python module definition
#[pymodule]
fn _pysof(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Add version from Cargo.toml
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;

    // Add functions
    m.add_function(wrap_pyfunction!(py_run_view_definition, m)?)?;
    m.add_function(wrap_pyfunction!(py_run_view_definition_with_options, m)?)?;
    m.add_function(wrap_pyfunction!(py_validate_view_definition, m)?)?;
    m.add_function(wrap_pyfunction!(py_validate_bundle, m)?)?;
    m.add_function(wrap_pyfunction!(py_parse_content_type, m)?)?;
    m.add_function(wrap_pyfunction!(py_get_supported_fhir_versions, m)?)?;
    m.add_function(wrap_pyfunction!(py_process_ndjson_to_file, m)?)?;

    // Add classes
    m.add_class::<ChunkedProcessor>()?;

    // Add exception classes with the Python names (not Py prefixed)
    m.add("SofError", m.py().get_type::<PySofError>())?;
    m.add(
        "InvalidViewDefinitionError",
        m.py().get_type::<PyInvalidViewDefinitionError>(),
    )?;
    m.add("FhirPathError", m.py().get_type::<PyFhirPathError>())?;
    m.add(
        "SerializationError",
        m.py().get_type::<PySerializationError>(),
    )?;
    m.add(
        "UnsupportedContentTypeError",
        m.py().get_type::<PyUnsupportedContentTypeError>(),
    )?;
    m.add("CsvError", m.py().get_type::<PyCsvError>())?;
    m.add("IoError", m.py().get_type::<PyIoError>())?;
    m.add(
        "InvalidSourceError",
        m.py().get_type::<PyInvalidSourceError>(),
    )?;
    m.add(
        "SourceNotFoundError",
        m.py().get_type::<PySourceNotFoundError>(),
    )?;
    m.add("SourceFetchError", m.py().get_type::<PySourceFetchError>())?;
    m.add("SourceReadError", m.py().get_type::<PySourceReadError>())?;
    m.add(
        "InvalidSourceContentError",
        m.py().get_type::<PyInvalidSourceContentError>(),
    )?;
    m.add(
        "UnsupportedSourceProtocolError",
        m.py().get_type::<PyUnsupportedSourceProtocolError>(),
    )?;

    Ok(())
}

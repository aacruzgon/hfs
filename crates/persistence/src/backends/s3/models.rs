use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::core::bulk_export::{ExportManifest, ExportProgress, ExportRequest};
use crate::core::bulk_submit::{SubmissionManifest, SubmissionSummary};
use crate::core::history::HistoryMethod;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryIndexEvent {
    pub resource_type: String,
    pub id: String,
    pub version_id: String,
    pub timestamp: DateTime<Utc>,
    pub method: HistoryMethod,
    pub deleted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportJobState {
    pub request: ExportRequest,
    pub progress: ExportProgress,
    pub manifest: Option<ExportManifest>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubmissionState {
    pub summary: SubmissionSummary,
    pub abort_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubmissionManifestState {
    pub manifest: SubmissionManifest,
}

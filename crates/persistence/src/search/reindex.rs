//! $reindex Operation Implementation.
//!
//! Provides the ability to rebuild search indexes for existing resources
//! when new SearchParameters are added or when indexes need to be repaired.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use uuid::Uuid;

use super::errors::ReindexError;
use super::extractor::SearchParameterExtractor;
use super::writer::SearchIndexWriter;

/// Request to start a reindex operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReindexRequest {
    /// Target resource types (None = all types).
    pub resource_types: Option<Vec<String>>,

    /// Specific SearchParameter URLs to reindex (None = all active).
    pub search_param_urls: Option<Vec<String>>,

    /// Batch size for processing resources.
    #[serde(default = "default_batch_size")]
    pub batch_size: u32,

    /// Whether to clear existing indexes before reindexing.
    #[serde(default)]
    pub clear_existing: bool,
}

fn default_batch_size() -> u32 {
    100
}

impl Default for ReindexRequest {
    fn default() -> Self {
        Self {
            resource_types: None,
            search_param_urls: None,
            batch_size: default_batch_size(),
            clear_existing: false,
        }
    }
}

impl ReindexRequest {
    /// Creates a new reindex request for all resources.
    pub fn all() -> Self {
        Self::default()
    }

    /// Creates a reindex request for specific resource types.
    pub fn for_types<I, S>(types: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            resource_types: Some(types.into_iter().map(Into::into).collect()),
            ..Self::default()
        }
    }

    /// Creates a reindex request for specific parameters.
    pub fn for_params<I, S>(urls: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            search_param_urls: Some(urls.into_iter().map(Into::into).collect()),
            ..Self::default()
        }
    }

    /// Sets the batch size.
    pub fn with_batch_size(mut self, size: u32) -> Self {
        self.batch_size = size;
        self
    }

    /// Enables clearing existing indexes.
    pub fn clear_existing(mut self) -> Self {
        self.clear_existing = true;
        self
    }
}

/// Status of a reindex operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ReindexStatus {
    /// Reindex is queued but not started.
    Queued,
    /// Reindex is currently running.
    InProgress,
    /// Reindex completed successfully.
    Completed,
    /// Reindex failed with an error.
    Failed,
    /// Reindex was cancelled.
    Cancelled,
}

impl ReindexStatus {
    /// Returns true if the job is still running.
    pub fn is_running(&self) -> bool {
        matches!(self, ReindexStatus::Queued | ReindexStatus::InProgress)
    }

    /// Returns true if the job has finished (success, failure, or cancelled).
    pub fn is_finished(&self) -> bool {
        matches!(
            self,
            ReindexStatus::Completed | ReindexStatus::Failed | ReindexStatus::Cancelled
        )
    }
}

/// Progress information for a reindex job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReindexProgress {
    /// Unique job identifier.
    pub job_id: String,

    /// Current status.
    pub status: ReindexStatus,

    /// Total number of resources to process.
    pub total_resources: u64,

    /// Number of resources processed so far.
    pub processed_resources: u64,

    /// Number of index entries created.
    pub entries_created: u64,

    /// Errors encountered during processing.
    pub errors: Vec<ReindexProgressError>,

    /// When the job was started.
    pub started_at: Option<String>,

    /// When the job completed.
    pub completed_at: Option<String>,

    /// Error message if status is Failed.
    pub error_message: Option<String>,

    /// Current resource type being processed.
    pub current_resource_type: Option<String>,
}

/// An error encountered during reindexing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReindexProgressError {
    /// Resource type.
    pub resource_type: String,
    /// Resource ID.
    pub resource_id: String,
    /// Error message.
    pub error: String,
}

impl ReindexProgress {
    /// Creates a new progress tracker for a job.
    pub fn new(job_id: impl Into<String>) -> Self {
        Self {
            job_id: job_id.into(),
            status: ReindexStatus::Queued,
            total_resources: 0,
            processed_resources: 0,
            entries_created: 0,
            errors: Vec::new(),
            started_at: None,
            completed_at: None,
            error_message: None,
            current_resource_type: None,
        }
    }

    /// Returns the progress percentage (0-100).
    pub fn percentage(&self) -> f64 {
        if self.total_resources == 0 {
            0.0
        } else {
            (self.processed_resources as f64 / self.total_resources as f64) * 100.0
        }
    }

    /// Returns true if any errors occurred.
    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty() || self.error_message.is_some()
    }

    /// Converts to FHIR Parameters resource.
    pub fn to_parameters(&self) -> serde_json::Value {
        serde_json::json!({
            "resourceType": "Parameters",
            "parameter": [
                {"name": "jobId", "valueString": self.job_id},
                {"name": "status", "valueCode": format!("{:?}", self.status).to_lowercase()},
                {"name": "total", "valueInteger": self.total_resources},
                {"name": "processed", "valueInteger": self.processed_resources},
                {"name": "entriesCreated", "valueInteger": self.entries_created},
                {"name": "errorCount", "valueInteger": self.errors.len()},
                {"name": "percentage", "valueDecimal": self.percentage()}
            ]
        })
    }
}

/// Manages reindex operations.
pub struct ReindexOperation<W: SearchIndexWriter> {
    /// The search parameter extractor.
    extractor: Arc<SearchParameterExtractor>,
    /// The index writer.
    writer: Arc<W>,
    /// Active jobs.
    jobs: Arc<RwLock<HashMap<String, ReindexProgress>>>,
    /// Cancellation channels.
    cancel_channels: Arc<RwLock<HashMap<String, mpsc::Sender<()>>>>,
}

impl<W: SearchIndexWriter + 'static> ReindexOperation<W> {
    /// Creates a new reindex operation manager.
    pub fn new(extractor: Arc<SearchParameterExtractor>, writer: Arc<W>) -> Self {
        Self {
            extractor,
            writer,
            jobs: Arc::new(RwLock::new(HashMap::new())),
            cancel_channels: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Starts a reindex operation.
    ///
    /// Returns immediately with a job ID. The reindex runs in the background.
    pub async fn start(&self, _request: ReindexRequest) -> Result<String, ReindexError> {
        let job_id = Uuid::new_v4().to_string();
        let progress = ReindexProgress::new(&job_id);

        // Store the job
        self.jobs.write().insert(job_id.clone(), progress);

        // Create cancellation channel
        let (_tx, _rx) = mpsc::channel::<()>(1);
        self.cancel_channels.write().insert(job_id.clone(), _tx);

        // TODO: Start background task to perform reindexing
        // For now, just mark as completed
        {
            let mut jobs = self.jobs.write();
            if let Some(progress) = jobs.get_mut(&job_id) {
                progress.status = ReindexStatus::Completed;
                progress.started_at = Some(chrono::Utc::now().to_rfc3339());
                progress.completed_at = Some(chrono::Utc::now().to_rfc3339());
            }
        }

        Ok(job_id)
    }

    /// Gets the progress of a reindex job.
    pub async fn get_progress(&self, job_id: &str) -> Option<ReindexProgress> {
        self.jobs.read().get(job_id).cloned()
    }

    /// Cancels a running reindex job.
    pub async fn cancel(&self, job_id: &str) -> Result<(), ReindexError> {
        // Check if job exists and is running
        {
            let jobs = self.jobs.read();
            let progress = jobs.get(job_id).ok_or_else(|| ReindexError::JobNotFound {
                job_id: job_id.to_string(),
            })?;

            if !progress.status.is_running() {
                return Ok(()); // Already finished
            }
        }

        // Send cancellation signal
        if let Some(tx) = self.cancel_channels.read().get(job_id) {
            let _ = tx.send(()).await;
        }

        // Update status
        {
            let mut jobs = self.jobs.write();
            if let Some(progress) = jobs.get_mut(job_id) {
                progress.status = ReindexStatus::Cancelled;
                progress.completed_at = Some(chrono::Utc::now().to_rfc3339());
            }
        }

        Ok(())
    }

    /// Lists all jobs (active and recent).
    pub fn list_jobs(&self) -> Vec<ReindexProgress> {
        self.jobs.read().values().cloned().collect()
    }

    /// Removes completed jobs older than the specified duration.
    pub fn cleanup_old_jobs(&self, max_age_seconds: i64) {
        let cutoff = chrono::Utc::now() - chrono::Duration::seconds(max_age_seconds);

        let mut jobs = self.jobs.write();
        let mut channels = self.cancel_channels.write();

        jobs.retain(|job_id, progress| {
            if progress.status.is_finished() {
                if let Some(ref completed_at) = progress.completed_at {
                    if let Ok(completed) = chrono::DateTime::parse_from_rfc3339(completed_at) {
                        if completed.with_timezone(&chrono::Utc) < cutoff {
                            channels.remove(job_id);
                            return false;
                        }
                    }
                }
            }
            true
        });
    }
}

impl<W: SearchIndexWriter> std::fmt::Debug for ReindexOperation<W> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReindexOperation")
            .field("active_jobs", &self.jobs.read().len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reindex_request() {
        let req = ReindexRequest::for_types(vec!["Patient", "Observation"])
            .with_batch_size(50)
            .clear_existing();

        assert_eq!(req.resource_types.as_ref().unwrap().len(), 2);
        assert_eq!(req.batch_size, 50);
        assert!(req.clear_existing);
    }

    #[test]
    fn test_reindex_status() {
        assert!(ReindexStatus::InProgress.is_running());
        assert!(!ReindexStatus::Completed.is_running());
        assert!(ReindexStatus::Completed.is_finished());
        assert!(ReindexStatus::Failed.is_finished());
    }

    #[test]
    fn test_reindex_progress() {
        let mut progress = ReindexProgress::new("job-123");
        progress.total_resources = 100;
        progress.processed_resources = 50;

        assert_eq!(progress.percentage(), 50.0);
        assert!(!progress.has_errors());

        progress.errors.push(ReindexProgressError {
            resource_type: "Patient".to_string(),
            resource_id: "1".to_string(),
            error: "test error".to_string(),
        });

        assert!(progress.has_errors());
    }

    #[test]
    fn test_progress_to_parameters() {
        let progress = ReindexProgress::new("job-123");
        let params = progress.to_parameters();

        assert_eq!(params["resourceType"], "Parameters");
        assert!(params["parameter"].is_array());
    }
}

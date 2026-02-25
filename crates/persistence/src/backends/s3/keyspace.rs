use chrono::{DateTime, Utc};

/// Keyspace builder for S3 object paths.
#[derive(Debug, Clone)]
pub struct S3Keyspace {
    base_prefix: Option<String>,
}

impl S3Keyspace {
    pub fn new(base_prefix: Option<String>) -> Self {
        let base_prefix = base_prefix
            .map(|p| p.trim_matches('/').to_string())
            .filter(|p| !p.is_empty());
        Self { base_prefix }
    }

    pub fn with_tenant_prefix(&self, tenant_id: &str) -> Self {
        let tenant = tenant_id.trim_matches('/');
        let merged = match &self.base_prefix {
            Some(base) => format!("{}/{}", base, tenant),
            None => tenant.to_string(),
        };
        Self::new(Some(merged))
    }

    pub fn current_resource_key(&self, resource_type: &str, id: &str) -> String {
        self.join(&["resources", resource_type, id, "current.json"])
    }

    pub fn history_version_key(&self, resource_type: &str, id: &str, version_id: &str) -> String {
        self.join(&[
            "resources",
            resource_type,
            id,
            "_history",
            &format!("{}.json", version_id),
        ])
    }

    pub fn history_versions_prefix(&self, resource_type: &str, id: &str) -> String {
        self.join(&["resources", resource_type, id, "_history/"])
    }

    pub fn resources_prefix(&self) -> String {
        self.join(&["resources/"])
    }

    pub fn resource_type_prefix(&self, resource_type: &str) -> String {
        self.join(&["resources", resource_type, "/"])
    }

    pub fn history_type_event_key(
        &self,
        resource_type: &str,
        timestamp: DateTime<Utc>,
        id: &str,
        version_id: &str,
        suffix: &str,
    ) -> String {
        self.join(&[
            "history",
            "type",
            resource_type,
            &format!(
                "{}_{}_{}_{}.json",
                timestamp.timestamp_millis(),
                sanitize(id),
                version_id,
                suffix
            ),
        ])
    }

    pub fn history_system_event_key(
        &self,
        resource_type: &str,
        timestamp: DateTime<Utc>,
        id: &str,
        version_id: &str,
        suffix: &str,
    ) -> String {
        self.join(&[
            "history",
            "system",
            &format!(
                "{}_{}_{}_{}_{}.json",
                timestamp.timestamp_millis(),
                sanitize(resource_type),
                sanitize(id),
                version_id,
                suffix
            ),
        ])
    }

    pub fn history_type_prefix(&self, resource_type: &str) -> String {
        self.join(&["history", "type", resource_type, "/"])
    }

    pub fn history_system_prefix(&self) -> String {
        self.join(&["history", "system/"])
    }

    pub fn export_job_state_key(&self, job_id: &str) -> String {
        self.join(&["bulk", "export", "jobs", job_id, "state.json"])
    }

    pub fn export_job_progress_key(&self, job_id: &str, resource_type: &str) -> String {
        self.join(&[
            "bulk",
            "export",
            "jobs",
            job_id,
            "progress",
            &format!("{}.json", resource_type),
        ])
    }

    pub fn export_job_manifest_key(&self, job_id: &str) -> String {
        self.join(&["bulk", "export", "jobs", job_id, "manifest.json"])
    }

    pub fn export_job_output_key(&self, job_id: &str, resource_type: &str, part: u32) -> String {
        self.join(&[
            "bulk",
            "export",
            "jobs",
            job_id,
            "output",
            resource_type,
            &format!("part-{}.ndjson", part),
        ])
    }

    pub fn export_jobs_prefix(&self) -> String {
        self.join(&["bulk", "export", "jobs/"])
    }

    pub fn export_job_prefix(&self, job_id: &str) -> String {
        self.join(&["bulk", "export", "jobs", job_id, "/"])
    }

    pub fn submit_state_key(&self, submitter: &str, submission_id: &str) -> String {
        self.join(&["bulk", "submit", submitter, submission_id, "state.json"])
    }

    pub fn submit_manifest_key(
        &self,
        submitter: &str,
        submission_id: &str,
        manifest_id: &str,
    ) -> String {
        self.join(&[
            "bulk",
            "submit",
            submitter,
            submission_id,
            "manifests",
            &format!("{}.json", manifest_id),
        ])
    }

    pub fn submit_raw_line_key(
        &self,
        submitter: &str,
        submission_id: &str,
        manifest_id: &str,
        line: u64,
    ) -> String {
        self.join(&[
            "bulk",
            "submit",
            submitter,
            submission_id,
            "raw",
            manifest_id,
            &format!("line-{}.ndjson", line),
        ])
    }

    pub fn submit_result_line_key(
        &self,
        submitter: &str,
        submission_id: &str,
        manifest_id: &str,
        line: u64,
    ) -> String {
        self.join(&[
            "bulk",
            "submit",
            submitter,
            submission_id,
            "results",
            manifest_id,
            &format!("line-{}.json", line),
        ])
    }

    pub fn submit_change_key(
        &self,
        submitter: &str,
        submission_id: &str,
        change_id: &str,
    ) -> String {
        self.join(&[
            "bulk",
            "submit",
            submitter,
            submission_id,
            "changes",
            &format!("{}.json", change_id),
        ])
    }

    pub fn submit_prefix(&self, submitter: &str, submission_id: &str) -> String {
        self.join(&["bulk", "submit", submitter, submission_id, "/"])
    }

    pub fn submit_root_prefix(&self) -> String {
        self.join(&["bulk", "submit/"])
    }

    fn join(&self, parts: &[&str]) -> String {
        let mut segs: Vec<String> = Vec::new();
        if let Some(prefix) = &self.base_prefix {
            segs.push(prefix.clone());
        }

        for part in parts {
            let trimmed = part.trim_matches('/');
            if trimmed.is_empty() {
                continue;
            }
            segs.push(trimmed.to_string());
        }

        let mut out = segs.join("/");
        if parts.last().map(|p| p.ends_with('/')).unwrap_or(false) && !out.ends_with('/') {
            out.push('/');
        }
        out
    }
}

fn sanitize(value: &str) -> String {
    value
        .chars()
        .map(|c| match c {
            '/' | '\\' | ' ' => '_',
            _ => c,
        })
        .collect()
}

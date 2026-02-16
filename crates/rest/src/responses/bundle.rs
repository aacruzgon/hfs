//! Bundle response building.
//!
//! Provides utilities for building FHIR Bundle responses.

use serde_json::Value;

/// Bundle types as defined by FHIR.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BundleType {
    /// Document bundle.
    Document,
    /// Message bundle.
    Message,
    /// Transaction request bundle.
    Transaction,
    /// Transaction response bundle.
    TransactionResponse,
    /// Batch request bundle.
    Batch,
    /// Batch response bundle.
    BatchResponse,
    /// Search results bundle.
    Searchset,
    /// History results bundle.
    History,
    /// Collection bundle.
    Collection,
}

impl BundleType {
    /// Returns the FHIR code string representation.
    pub fn as_str(&self) -> &'static str {
        match self {
            BundleType::Document => "document",
            BundleType::Message => "message",
            BundleType::Transaction => "transaction",
            BundleType::TransactionResponse => "transaction-response",
            BundleType::Batch => "batch",
            BundleType::BatchResponse => "batch-response",
            BundleType::Searchset => "searchset",
            BundleType::History => "history",
            BundleType::Collection => "collection",
        }
    }
}

/// Search mode for bundle entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchMode {
    /// Primary search result.
    Match,
    /// Included via _include.
    Include,
    /// Result of server-side processing.
    Outcome,
}

impl SearchMode {
    /// Returns the FHIR code string representation.
    pub fn as_str(&self) -> &'static str {
        match self {
            SearchMode::Match => "match",
            SearchMode::Include => "include",
            SearchMode::Outcome => "outcome",
        }
    }
}

/// A link in a Bundle.
#[derive(Debug, Clone)]
pub struct BundleLink {
    /// The relation type (self, next, previous, first, last).
    pub relation: String,
    /// The URL.
    pub url: String,
}

impl BundleLink {
    /// Creates a new link.
    pub fn new(relation: impl Into<String>, url: impl Into<String>) -> Self {
        Self {
            relation: relation.into(),
            url: url.into(),
        }
    }

    /// Creates a self link.
    pub fn self_link(url: impl Into<String>) -> Self {
        Self::new("self", url)
    }

    /// Creates a next link.
    pub fn next(url: impl Into<String>) -> Self {
        Self::new("next", url)
    }

    /// Creates a previous link.
    pub fn previous(url: impl Into<String>) -> Self {
        Self::new("previous", url)
    }

    /// Converts to FHIR JSON.
    pub fn to_json(&self) -> Value {
        serde_json::json!({
            "relation": self.relation,
            "url": self.url
        })
    }
}

/// An entry in a Bundle.
#[derive(Debug, Clone)]
pub struct BundleEntry {
    /// Full URL of the resource.
    pub full_url: Option<String>,
    /// The resource itself.
    pub resource: Option<Value>,
    /// Search mode (for searchset bundles).
    pub search_mode: Option<SearchMode>,
    /// Request information (for transaction/batch).
    pub request: Option<BundleEntryRequest>,
    /// Response information (for transaction/batch response).
    pub response: Option<BundleEntryResponse>,
}

/// Request information in a bundle entry.
#[derive(Debug, Clone)]
pub struct BundleEntryRequest {
    /// HTTP method.
    pub method: String,
    /// URL.
    pub url: String,
    /// If-Match header.
    pub if_match: Option<String>,
    /// If-None-Exist header.
    pub if_none_exist: Option<String>,
}

/// Response information in a bundle entry.
#[derive(Debug, Clone)]
pub struct BundleEntryResponse {
    /// HTTP status.
    pub status: String,
    /// Location header.
    pub location: Option<String>,
    /// ETag header.
    pub etag: Option<String>,
    /// Last-Modified header.
    pub last_modified: Option<String>,
}

impl BundleEntry {
    /// Creates a new entry with just a resource.
    pub fn with_resource(resource: Value, full_url: impl Into<String>) -> Self {
        Self {
            full_url: Some(full_url.into()),
            resource: Some(resource),
            search_mode: None,
            request: None,
            response: None,
        }
    }

    /// Creates a search result entry.
    pub fn search_result(resource: Value, full_url: impl Into<String>) -> Self {
        Self {
            full_url: Some(full_url.into()),
            resource: Some(resource),
            search_mode: Some(SearchMode::Match),
            request: None,
            response: None,
        }
    }

    /// Creates an included resource entry.
    pub fn included(resource: Value, full_url: impl Into<String>) -> Self {
        Self {
            full_url: Some(full_url.into()),
            resource: Some(resource),
            search_mode: Some(SearchMode::Include),
            request: None,
            response: None,
        }
    }

    /// Sets the search mode.
    pub fn with_search_mode(mut self, mode: SearchMode) -> Self {
        self.search_mode = Some(mode);
        self
    }

    /// Converts to FHIR JSON.
    pub fn to_json(&self) -> Value {
        let mut entry = serde_json::json!({});

        if let Some(url) = &self.full_url {
            entry["fullUrl"] = serde_json::json!(url);
        }

        if let Some(resource) = &self.resource {
            entry["resource"] = resource.clone();
        }

        if let Some(mode) = &self.search_mode {
            entry["search"] = serde_json::json!({
                "mode": mode.as_str()
            });
        }

        if let Some(request) = &self.request {
            let mut req = serde_json::json!({
                "method": request.method,
                "url": request.url
            });
            if let Some(if_match) = &request.if_match {
                req["ifMatch"] = serde_json::json!(if_match);
            }
            if let Some(if_none_exist) = &request.if_none_exist {
                req["ifNoneExist"] = serde_json::json!(if_none_exist);
            }
            entry["request"] = req;
        }

        if let Some(response) = &self.response {
            let mut resp = serde_json::json!({
                "status": response.status
            });
            if let Some(location) = &response.location {
                resp["location"] = serde_json::json!(location);
            }
            if let Some(etag) = &response.etag {
                resp["etag"] = serde_json::json!(etag);
            }
            if let Some(last_modified) = &response.last_modified {
                resp["lastModified"] = serde_json::json!(last_modified);
            }
            entry["response"] = resp;
        }

        entry
    }
}

/// Builder for Bundle resources.
#[derive(Debug)]
pub struct BundleBuilder {
    bundle_type: BundleType,
    total: Option<usize>,
    links: Vec<BundleLink>,
    entries: Vec<BundleEntry>,
    timestamp: Option<String>,
}

impl BundleBuilder {
    /// Creates a new builder for a specific bundle type.
    pub fn new(bundle_type: BundleType) -> Self {
        Self {
            bundle_type,
            total: None,
            links: Vec::new(),
            entries: Vec::new(),
            timestamp: None,
        }
    }

    /// Creates a searchset bundle builder.
    pub fn searchset() -> Self {
        Self::new(BundleType::Searchset)
    }

    /// Creates a history bundle builder.
    pub fn history() -> Self {
        Self::new(BundleType::History)
    }

    /// Creates a batch response bundle builder.
    pub fn batch_response() -> Self {
        Self::new(BundleType::BatchResponse)
    }

    /// Creates a transaction response bundle builder.
    pub fn transaction_response() -> Self {
        Self::new(BundleType::TransactionResponse)
    }

    /// Sets the total count.
    pub fn total(mut self, count: usize) -> Self {
        self.total = Some(count);
        self
    }

    /// Adds a link.
    pub fn add_link(mut self, link: BundleLink) -> Self {
        self.links.push(link);
        self
    }

    /// Adds a self link.
    pub fn self_link(self, url: impl Into<String>) -> Self {
        self.add_link(BundleLink::self_link(url))
    }

    /// Adds an entry.
    pub fn add_entry(mut self, entry: BundleEntry) -> Self {
        self.entries.push(entry);
        self
    }

    /// Sets the timestamp.
    pub fn timestamp(mut self, ts: impl Into<String>) -> Self {
        self.timestamp = Some(ts.into());
        self
    }

    /// Builds the Bundle resource.
    pub fn build(self) -> Value {
        let mut bundle = serde_json::json!({
            "resourceType": "Bundle",
            "type": self.bundle_type.as_str()
        });

        if let Some(total) = self.total {
            bundle["total"] = serde_json::json!(total);
        }

        if !self.links.is_empty() {
            bundle["link"] =
                serde_json::json!(self.links.iter().map(|l| l.to_json()).collect::<Vec<_>>());
        }

        if !self.entries.is_empty() {
            bundle["entry"] =
                serde_json::json!(self.entries.iter().map(|e| e.to_json()).collect::<Vec<_>>());
        }

        if let Some(ts) = self.timestamp {
            bundle["timestamp"] = serde_json::json!(ts);
        }

        bundle
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_searchset_bundle() {
        let patient = serde_json::json!({
            "resourceType": "Patient",
            "id": "123"
        });

        let bundle = BundleBuilder::searchset()
            .total(1)
            .self_link("http://example.com/Patient")
            .add_entry(BundleEntry::search_result(
                patient,
                "http://example.com/Patient/123",
            ))
            .build();

        assert_eq!(bundle["resourceType"], "Bundle");
        assert_eq!(bundle["type"], "searchset");
        assert_eq!(bundle["total"], 1);
        assert_eq!(bundle["entry"][0]["search"]["mode"], "match");
    }

    #[test]
    fn test_bundle_link() {
        let link = BundleLink::next("http://example.com/Patient?page=2");
        let json = link.to_json();

        assert_eq!(json["relation"], "next");
        assert_eq!(json["url"], "http://example.com/Patient?page=2");
    }

    #[test]
    fn test_bundle_type_as_str() {
        assert_eq!(BundleType::Searchset.as_str(), "searchset");
        assert_eq!(BundleType::History.as_str(), "history");
        assert_eq!(
            BundleType::TransactionResponse.as_str(),
            "transaction-response"
        );
    }
}

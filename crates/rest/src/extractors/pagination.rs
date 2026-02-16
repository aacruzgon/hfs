//! Pagination extractor.
//!
//! Extracts pagination parameters from requests.

use axum::{
    extract::{FromRequestParts, Query},
    http::{StatusCode, request::Parts},
};
use serde::Deserialize;

/// Axum extractor for pagination parameters.
///
/// Extracts and validates _count and _offset parameters.
///
/// # Example
///
/// ```rust,ignore
/// use helios_rest::extractors::Pagination;
///
/// async fn list_handler(pagination: Pagination) {
///     let page_size = pagination.count();
///     let offset = pagination.offset();
/// }
/// ```
#[derive(Debug, Clone)]
pub struct Pagination {
    /// Page size (number of items to return).
    count: usize,
    /// Offset (number of items to skip).
    offset: usize,
    /// Maximum allowed page size.
    max_count: usize,
}

/// Query parameters for pagination.
#[derive(Debug, Deserialize)]
struct PaginationQuery {
    #[serde(rename = "_count")]
    count: Option<usize>,
    #[serde(rename = "_offset")]
    offset: Option<usize>,
}

impl Pagination {
    /// Creates a new Pagination with the given values.
    pub fn new(count: usize, offset: usize, max_count: usize) -> Self {
        Self {
            count: count.min(max_count),
            offset,
            max_count,
        }
    }

    /// Creates a Pagination with default values.
    pub fn default_with_limits(default_count: usize, max_count: usize) -> Self {
        Self {
            count: default_count,
            offset: 0,
            max_count,
        }
    }

    /// Returns the page size.
    pub fn count(&self) -> usize {
        self.count
    }

    /// Returns the offset.
    pub fn offset(&self) -> usize {
        self.offset
    }

    /// Returns the maximum page size.
    pub fn max_count(&self) -> usize {
        self.max_count
    }

    /// Returns the current page number (0-indexed).
    pub fn page(&self) -> usize {
        self.offset.checked_div(self.count).unwrap_or(0)
    }

    /// Creates pagination for the next page.
    pub fn next_page(&self) -> Self {
        Self {
            count: self.count,
            offset: self.offset + self.count,
            max_count: self.max_count,
        }
    }

    /// Creates pagination for the previous page.
    pub fn prev_page(&self) -> Option<Self> {
        if self.offset >= self.count {
            Some(Self {
                count: self.count,
                offset: self.offset - self.count,
                max_count: self.max_count,
            })
        } else if self.offset > 0 {
            Some(Self {
                count: self.count,
                offset: 0,
                max_count: self.max_count,
            })
        } else {
            None
        }
    }
}

impl Default for Pagination {
    fn default() -> Self {
        Self {
            count: 20,
            offset: 0,
            max_count: 1000,
        }
    }
}

impl<S> FromRequestParts<S> for Pagination
where
    S: Send + Sync,
{
    type Rejection = (StatusCode, &'static str);

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let Query(query) = Query::<PaginationQuery>::from_request_parts(parts, state)
            .await
            .map_err(|_| (StatusCode::BAD_REQUEST, "Invalid pagination parameters"))?;

        // Default values
        let default_count = 20;
        let max_count = 1000;

        let count = query.count.unwrap_or(default_count).min(max_count);
        let offset = query.offset.unwrap_or(0);

        Ok(Pagination::new(count, offset, max_count))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let pagination = Pagination::new(10, 20, 100);
        assert_eq!(pagination.count(), 10);
        assert_eq!(pagination.offset(), 20);
    }

    #[test]
    fn test_count_capped_at_max() {
        let pagination = Pagination::new(200, 0, 100);
        assert_eq!(pagination.count(), 100);
    }

    #[test]
    fn test_page() {
        let pagination = Pagination::new(10, 30, 100);
        assert_eq!(pagination.page(), 3);
    }

    #[test]
    fn test_next_page() {
        let pagination = Pagination::new(10, 0, 100);
        let next = pagination.next_page();
        assert_eq!(next.offset(), 10);
    }

    #[test]
    fn test_prev_page() {
        let pagination = Pagination::new(10, 30, 100);
        let prev = pagination.prev_page().unwrap();
        assert_eq!(prev.offset(), 20);
    }

    #[test]
    fn test_prev_page_at_start() {
        let pagination = Pagination::new(10, 0, 100);
        assert!(pagination.prev_page().is_none());
    }
}

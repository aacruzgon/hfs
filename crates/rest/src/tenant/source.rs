//! Tenant source identification.
//!
//! Defines the sources from which tenant information can be extracted.

use std::fmt;

/// Source from which tenant information was extracted.
///
/// Sources are listed in priority order (highest to lowest):
/// 1. URL path prefix (`/{tenant}/...`)
/// 2. X-Tenant-ID header
/// 3. JWT token claim (future)
/// 4. Default tenant from configuration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TenantSource {
    /// Tenant extracted from URL path prefix (highest priority).
    UrlPath,
    /// Tenant extracted from X-Tenant-ID header.
    Header,
    /// Tenant extracted from JWT token claim (future use).
    JwtClaim,
    /// Default tenant from configuration (lowest priority).
    Default,
}

impl TenantSource {
    /// Returns the priority of this source (higher = more authoritative).
    pub fn priority(&self) -> u8 {
        match self {
            TenantSource::UrlPath => 4,
            TenantSource::Header => 3,
            TenantSource::JwtClaim => 2,
            TenantSource::Default => 1,
        }
    }

    /// Returns true if this source is URL-based.
    pub fn is_url_based(&self) -> bool {
        matches!(self, TenantSource::UrlPath)
    }

    /// Returns true if this source is the default fallback.
    pub fn is_default(&self) -> bool {
        matches!(self, TenantSource::Default)
    }
}

impl fmt::Display for TenantSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TenantSource::UrlPath => write!(f, "url_path"),
            TenantSource::Header => write!(f, "header"),
            TenantSource::JwtClaim => write!(f, "jwt_claim"),
            TenantSource::Default => write!(f, "default"),
        }
    }
}

impl Ord for TenantSource {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.priority().cmp(&other.priority())
    }
}

impl PartialOrd for TenantSource {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_source_priority() {
        assert!(TenantSource::UrlPath > TenantSource::Header);
        assert!(TenantSource::Header > TenantSource::JwtClaim);
        assert!(TenantSource::JwtClaim > TenantSource::Default);
    }

    #[test]
    fn test_source_display() {
        assert_eq!(TenantSource::UrlPath.to_string(), "url_path");
        assert_eq!(TenantSource::Header.to_string(), "header");
        assert_eq!(TenantSource::JwtClaim.to_string(), "jwt_claim");
        assert_eq!(TenantSource::Default.to_string(), "default");
    }

    #[test]
    fn test_is_url_based() {
        assert!(TenantSource::UrlPath.is_url_based());
        assert!(!TenantSource::Header.is_url_based());
        assert!(!TenantSource::JwtClaim.is_url_based());
        assert!(!TenantSource::Default.is_url_based());
    }

    #[test]
    fn test_is_default() {
        assert!(!TenantSource::UrlPath.is_default());
        assert!(!TenantSource::Header.is_default());
        assert!(!TenantSource::JwtClaim.is_default());
        assert!(TenantSource::Default.is_default());
    }
}

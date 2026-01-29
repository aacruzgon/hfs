//! Tenant validation for strict mode.
//!
//! Provides validation logic for ensuring tenant consistency when
//! multiple sources provide tenant information.

use helios_persistence::tenant::TenantId;

use super::resolver::ResolvedTenant;
use super::source::TenantSource;

/// Error when tenant sources disagree in strict validation mode.
#[derive(Debug, Clone)]
pub struct TenantMismatchError {
    /// The tenant ID from the primary source.
    pub primary_tenant: TenantId,
    /// The primary source.
    pub primary_source: TenantSource,
    /// Conflicting tenant ID.
    pub conflicting_tenant: TenantId,
    /// Source of the conflicting tenant.
    pub conflicting_source: TenantSource,
}

impl std::fmt::Display for TenantMismatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Tenant mismatch: {} from {} conflicts with {} from {}",
            self.primary_tenant.as_str(),
            self.primary_source,
            self.conflicting_tenant.as_str(),
            self.conflicting_source
        )
    }
}

impl std::error::Error for TenantMismatchError {}

/// Validates tenant consistency across sources.
pub struct TenantValidator;

impl TenantValidator {
    /// Validates that all sources agree on the tenant ID.
    ///
    /// In strict mode, if multiple sources provide a tenant ID but they
    /// disagree, this returns an error.
    ///
    /// # Arguments
    ///
    /// * `resolved` - The resolved tenant information
    ///
    /// # Returns
    ///
    /// `Ok(())` if validation passes, or `Err(TenantMismatchError)` if
    /// sources disagree.
    pub fn validate_consistency(resolved: &ResolvedTenant) -> Result<(), TenantMismatchError> {
        // If we only have one source (or none), there's no conflict
        if resolved.all_sources.len() <= 1 {
            return Ok(());
        }

        // Check that all sources agree on the tenant ID
        let primary = &resolved.all_sources[0];

        for (source, tenant_id) in resolved.all_sources.iter().skip(1) {
            if tenant_id.as_str() != primary.1.as_str() {
                return Err(TenantMismatchError {
                    primary_tenant: primary.1.clone(),
                    primary_source: primary.0,
                    conflicting_tenant: tenant_id.clone(),
                    conflicting_source: *source,
                });
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_single_source() {
        let resolved = ResolvedTenant {
            tenant_id: TenantId::new("acme"),
            source: TenantSource::Header,
            all_sources: vec![(TenantSource::Header, TenantId::new("acme"))],
        };

        assert!(TenantValidator::validate_consistency(&resolved).is_ok());
    }

    #[test]
    fn test_validate_consistent_sources() {
        let resolved = ResolvedTenant {
            tenant_id: TenantId::new("acme"),
            source: TenantSource::UrlPath,
            all_sources: vec![
                (TenantSource::UrlPath, TenantId::new("acme")),
                (TenantSource::Header, TenantId::new("acme")),
            ],
        };

        assert!(TenantValidator::validate_consistency(&resolved).is_ok());
    }

    #[test]
    fn test_validate_conflicting_sources() {
        let resolved = ResolvedTenant {
            tenant_id: TenantId::new("acme"),
            source: TenantSource::UrlPath,
            all_sources: vec![
                (TenantSource::UrlPath, TenantId::new("acme")),
                (TenantSource::Header, TenantId::new("other")),
            ],
        };

        let result = TenantValidator::validate_consistency(&resolved);
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert_eq!(err.primary_tenant.as_str(), "acme");
        assert_eq!(err.primary_source, TenantSource::UrlPath);
        assert_eq!(err.conflicting_tenant.as_str(), "other");
        assert_eq!(err.conflicting_source, TenantSource::Header);
    }

    #[test]
    fn test_validate_default_source_only() {
        let resolved = ResolvedTenant {
            tenant_id: TenantId::new("default"),
            source: TenantSource::Default,
            all_sources: vec![],
        };

        assert!(TenantValidator::validate_consistency(&resolved).is_ok());
    }

    #[test]
    fn test_error_display() {
        let err = TenantMismatchError {
            primary_tenant: TenantId::new("acme"),
            primary_source: TenantSource::UrlPath,
            conflicting_tenant: TenantId::new("other"),
            conflicting_source: TenantSource::Header,
        };

        let msg = err.to_string();
        assert!(msg.contains("acme"));
        assert!(msg.contains("other"));
        assert!(msg.contains("url_path"));
        assert!(msg.contains("header"));
    }
}

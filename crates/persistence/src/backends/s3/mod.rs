//! S3 backend implementation.
//!
//! Phase 1 scope includes [`Backend`] and [`ResourceStorage`] CRUD support.

mod backend;
mod storage;
mod versioned;

pub use backend::{S3Backend, S3BackendConfig, S3TenancyMode};

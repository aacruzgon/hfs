//! Test that num_threads parameter is properly passed through to RunOptions

use helios_sof::RunOptions;
use chrono::{DateTime, Utc};

#[test]
fn test_run_options_with_num_threads() {
    // Test that we can construct RunOptions with num_threads
    let options = RunOptions {
        since: None,
        limit: Some(100),
        page: Some(1),
        num_threads: Some(8),
        ..Default::default()
    };

    assert_eq!(options.limit, Some(100));
    assert_eq!(options.page, Some(1));
    assert_eq!(options.num_threads, Some(8));
    assert!(options.since.is_none());
}

#[test]
fn test_run_options_default_num_threads() {
    // Test that default RunOptions has None for num_threads
    let options = RunOptions::default();
    
    assert!(options.num_threads.is_none());
    assert!(options.since.is_none());
    assert!(options.limit.is_none());
    assert!(options.page.is_none());
}

#[test]
fn test_run_options_partial_update() {
    // Test that we can update specific fields while keeping defaults
    let options = RunOptions {
        num_threads: Some(4),
        limit: Some(50),
        ..Default::default()
    };

    assert_eq!(options.num_threads, Some(4));
    assert_eq!(options.limit, Some(50));
    assert!(options.since.is_none());
    assert!(options.page.is_none());
}

#[test]
fn test_run_options_with_since() {
    // Test that we can combine num_threads with other options like since
    let since_time = "2024-01-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
    
    let options = RunOptions {
        since: Some(since_time),
        num_threads: Some(16),
        ..Default::default()
    };

    assert_eq!(options.num_threads, Some(16));
    assert_eq!(options.since, Some(since_time));
    assert!(options.limit.is_none());
    assert!(options.page.is_none());
}
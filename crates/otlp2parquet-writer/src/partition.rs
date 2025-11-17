//! Partition path generation for time-based organization
//!
//! Generates Hive-style partition paths:
//! logs/{service}/year={year}/month={month}/day={day}/hour={hour}/{uuid}-{timestamp}.parquet

use chrono::{DateTime, Datelike, Timelike, Utc};

/// Generate a partition path for a log entry
///
/// Format: `logs/{service_name}/year={year}/month={month}/day={day}/hour={hour}/{uuid}-{timestamp}.parquet`
///
/// # Arguments
/// * `service_name` - Service name from resource attributes
/// * `timestamp_nanos` - Log timestamp in nanoseconds since Unix epoch
///
/// # Returns
/// Partition path string
#[allow(dead_code)]
pub fn generate_partition_path(service_name: &str, timestamp_nanos: i64, hash_hex: &str) -> String {
    generate_partition_path_with_signal("logs", service_name, timestamp_nanos, hash_hex, None)
}

/// Generate a partition path with custom signal type and optional subdirectory
///
/// Format: `{signal_type}/{subdirectory}/{service_name}/year={year}/month={month}/day={day}/hour={hour}/{timestamp}-{hash}.parquet`
///
/// # Arguments
/// * `signal_type` - Signal type (logs, metrics, traces)
/// * `service_name` - Service name from resource attributes
/// * `timestamp_nanos` - Timestamp in nanoseconds since Unix epoch
/// * `hash_hex` - Content hash in hex format
/// * `subdirectory` - Optional subdirectory (e.g., metric type for metrics)
///
/// # Returns
/// Partition path string
#[allow(dead_code)]
pub fn generate_partition_path_with_signal(
    signal_type: &str,
    service_name: &str,
    timestamp_nanos: i64,
    hash_hex: &str,
    subdirectory: Option<&str>,
) -> String {
    // Convert nanoseconds to DateTime
    let timestamp_secs = timestamp_nanos / 1_000_000_000;
    let dt = DateTime::from_timestamp(timestamp_secs, 0).unwrap_or_else(Utc::now);

    // Use hash prefix for deterministic idempotent filenames
    let hash_prefix = if hash_hex.len() >= 16 {
        &hash_hex[..16]
    } else {
        hash_hex
    };
    let filename = format!("{}-{}.parquet", timestamp_nanos, hash_prefix);

    // Build base path with signal type
    let mut path = signal_type.to_string();

    // Add subdirectory if provided (e.g., gauge, sum, histogram for metrics)
    if let Some(subdir) = subdirectory {
        path.push('/');
        path.push_str(subdir);
    }

    // Add remaining partition components
    format!(
        "{}/{}/year={}/month={:02}/day={:02}/hour={:02}/{}",
        path,
        sanitize_service_name(service_name),
        dt.year(),
        dt.month(),
        dt.day(),
        dt.hour(),
        filename
    )
}

/// Sanitize service name for use in file paths
///
/// Replaces special characters with underscores to ensure valid paths
fn sanitize_service_name(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_partition_path() {
        // 2024-01-15 14:30:00 UTC in nanoseconds
        let timestamp_nanos = 1_705_327_800_000_000_000;
        let service_name = "my-service";

        let path = generate_partition_path(
            service_name,
            timestamp_nanos,
            "deadbeefdeadbeefdeadbeefdeadbeef",
        );

        assert!(path.starts_with("logs/my-service/"));
        assert!(path.contains("year=2024"));
        assert!(path.contains("month=01"));
        assert!(path.contains("day=15"));
        assert!(path.contains("hour=14"));
        assert!(path.ends_with(".parquet"));
        assert!(path.contains("deadbeefdeadbeef"));
    }

    #[test]
    fn test_sanitize_service_name() {
        assert_eq!(sanitize_service_name("my-service"), "my-service");
        assert_eq!(sanitize_service_name("my.service"), "my_service");
        assert_eq!(sanitize_service_name("my service"), "my_service");
        assert_eq!(sanitize_service_name("my/service"), "my_service");
    }
}

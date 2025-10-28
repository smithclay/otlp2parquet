// Partition path generation for time-based organization
//
// Generates Hive-style partition paths:
// logs/{service}/year={year}/month={month}/day={day}/hour={hour}/{uuid}-{timestamp}.parquet

use chrono::{DateTime, Datelike, Timelike, Utc};
use uuid::Uuid;

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
pub fn generate_partition_path(service_name: &str, timestamp_nanos: i64) -> String {
    // Convert nanoseconds to DateTime
    let timestamp_secs = timestamp_nanos / 1_000_000_000;
    let dt = DateTime::from_timestamp(timestamp_secs, 0).unwrap_or_else(Utc::now);

    // Generate unique filename
    let uuid = Uuid::new_v4();
    let filename = format!("{}-{}.parquet", uuid, timestamp_nanos);

    // Build partition path
    format!(
        "logs/{}/year={}/month={:02}/day={:02}/hour={:02}/{}",
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

        let path = generate_partition_path(service_name, timestamp_nanos);

        assert!(path.starts_with("logs/my-service/"));
        assert!(path.contains("year=2024"));
        assert!(path.contains("month=01"));
        assert!(path.contains("day=15"));
        assert!(path.contains("hour=14"));
        assert!(path.ends_with(".parquet"));
    }

    #[test]
    fn test_sanitize_service_name() {
        assert_eq!(sanitize_service_name("my-service"), "my-service");
        assert_eq!(sanitize_service_name("my.service"), "my_service");
        assert_eq!(sanitize_service_name("my service"), "my_service");
        assert_eq!(sanitize_service_name("my/service"), "my_service");
    }
}

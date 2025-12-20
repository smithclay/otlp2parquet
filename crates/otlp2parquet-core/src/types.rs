//! Shared types used across storage and iceberg crates
//!
//! These types are defined here to avoid circular dependencies

use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

/// OpenTelemetry signal types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SignalType {
    /// Logs signal
    Logs,
    /// Traces signal
    Traces,
    /// Metrics signal
    Metrics,
}

/// Metric data point types (the 5 OTLP metric kinds)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MetricType {
    Gauge,
    Sum,
    Histogram,
    ExponentialHistogram,
    Summary,
}

impl MetricType {
    /// Returns the string representation used in file paths and table names
    pub fn as_str(&self) -> &'static str {
        match self {
            MetricType::Gauge => "gauge",
            MetricType::Sum => "sum",
            MetricType::Histogram => "histogram",
            MetricType::ExponentialHistogram => "exponential_histogram",
            MetricType::Summary => "summary",
        }
    }
}

impl fmt::Display for MetricType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for MetricType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "gauge" => Ok(MetricType::Gauge),
            "sum" => Ok(MetricType::Sum),
            "histogram" => Ok(MetricType::Histogram),
            "exponential_histogram" => Ok(MetricType::ExponentialHistogram),
            "summary" => Ok(MetricType::Summary),
            _ => Err(format!("unknown metric type: {}", s)),
        }
    }
}

/// Unified signal identifier combining signal type and optional metric type.
///
/// This replaces string-based signal identifiers like "metrics:gauge" with
/// a typed enum that can be parsed and formatted consistently.
///
/// # Examples
/// ```
/// use otlp2parquet_core::types::SignalKey;
/// use std::str::FromStr;
///
/// // Parse from DO ID format
/// let key = SignalKey::from_str("metrics:gauge").unwrap();
/// assert!(matches!(key, SignalKey::Metrics(_)));
///
/// // Format back to string
/// assert_eq!(key.to_string(), "metrics:gauge");
///
/// // Simple signals
/// let logs = SignalKey::from_str("logs").unwrap();
/// assert_eq!(logs, SignalKey::Logs);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SignalKey {
    Logs,
    Traces,
    Metrics(MetricType),
}

impl SignalKey {
    /// Returns the base signal type
    pub fn signal_type(&self) -> SignalType {
        match self {
            SignalKey::Logs => SignalType::Logs,
            SignalKey::Traces => SignalType::Traces,
            SignalKey::Metrics(_) => SignalType::Metrics,
        }
    }

    /// Returns the metric type if this is a metrics signal
    pub fn metric_type(&self) -> Option<MetricType> {
        match self {
            SignalKey::Metrics(mt) => Some(*mt),
            _ => None,
        }
    }

    /// Returns the Iceberg table name for this signal
    pub fn table_name(&self) -> String {
        match self {
            SignalKey::Logs => "otel_logs".to_string(),
            SignalKey::Traces => "otel_traces".to_string(),
            SignalKey::Metrics(mt) => format!("otel_metrics_{}", mt.as_str()),
        }
    }

    /// Returns the analytics/metrics label for this signal
    pub fn analytics_label(&self) -> &'static str {
        match self {
            SignalKey::Logs => "logs",
            SignalKey::Traces => "traces",
            SignalKey::Metrics(MetricType::Gauge) => "metrics_gauge",
            SignalKey::Metrics(MetricType::Sum) => "metrics_sum",
            SignalKey::Metrics(MetricType::Histogram) => "metrics_histogram",
            SignalKey::Metrics(MetricType::ExponentialHistogram) => "metrics_exp_histogram",
            SignalKey::Metrics(MetricType::Summary) => "metrics_summary",
        }
    }
}

impl fmt::Display for SignalKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SignalKey::Logs => f.write_str("logs"),
            SignalKey::Traces => f.write_str("traces"),
            SignalKey::Metrics(mt) => write!(f, "metrics:{}", mt.as_str()),
        }
    }
}

impl FromStr for SignalKey {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(metric_str) = s.strip_prefix("metrics:") {
            let mt = MetricType::from_str(metric_str)?;
            Ok(SignalKey::Metrics(mt))
        } else {
            match s {
                "logs" => Ok(SignalKey::Logs),
                "traces" => Ok(SignalKey::Traces),
                "metrics" => Err("metrics signal requires type (e.g., metrics:gauge)".to_string()),
                _ => Err(format!("unknown signal: {}", s)),
            }
        }
    }
}

/// Blake3 content hash for deduplication
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Blake3Hash([u8; 32]);

impl Blake3Hash {
    pub fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metric_type_roundtrip() {
        let types = [
            MetricType::Gauge,
            MetricType::Sum,
            MetricType::Histogram,
            MetricType::ExponentialHistogram,
            MetricType::Summary,
        ];
        for mt in types {
            let s = mt.as_str();
            let parsed = MetricType::from_str(s).unwrap();
            assert_eq!(parsed, mt, "Roundtrip failed for {:?}", mt);
        }
    }

    #[test]
    fn test_signal_key_table_names() {
        assert_eq!(SignalKey::Logs.table_name(), "otel_logs");
        assert_eq!(SignalKey::Traces.table_name(), "otel_traces");
        assert_eq!(
            SignalKey::Metrics(MetricType::Gauge).table_name(),
            "otel_metrics_gauge"
        );
        assert_eq!(
            SignalKey::Metrics(MetricType::Sum).table_name(),
            "otel_metrics_sum"
        );
        assert_eq!(
            SignalKey::Metrics(MetricType::Histogram).table_name(),
            "otel_metrics_histogram"
        );
        assert_eq!(
            SignalKey::Metrics(MetricType::ExponentialHistogram).table_name(),
            "otel_metrics_exponential_histogram"
        );
        assert_eq!(
            SignalKey::Metrics(MetricType::Summary).table_name(),
            "otel_metrics_summary"
        );
    }

    #[test]
    fn test_signal_key_analytics_labels() {
        assert_eq!(SignalKey::Logs.analytics_label(), "logs");
        assert_eq!(SignalKey::Traces.analytics_label(), "traces");
        assert_eq!(
            SignalKey::Metrics(MetricType::Gauge).analytics_label(),
            "metrics_gauge"
        );
        assert_eq!(
            SignalKey::Metrics(MetricType::ExponentialHistogram).analytics_label(),
            "metrics_exp_histogram"
        );
    }

    #[test]
    fn test_signal_key_roundtrip() {
        let cases = [
            "logs",
            "traces",
            "metrics:gauge",
            "metrics:exponential_histogram",
        ];
        for input in cases {
            let key = SignalKey::from_str(input).unwrap();
            assert_eq!(key.to_string(), input, "Roundtrip failed for {}", input);
        }
    }

    #[test]
    fn test_signal_key_errors() {
        assert!(SignalKey::from_str("metrics").is_err()); // Missing metric type
        assert!(SignalKey::from_str("unknown").is_err()); // Unknown signal
        assert!(SignalKey::from_str("metrics:unknown").is_err()); // Unknown metric type
    }
}

/// Result of writing a Parquet file
///
/// Contains metadata needed for both storage tracking and Iceberg catalog commits
#[derive(Clone)]
pub struct ParquetWriteResult {
    /// Path where the file was written
    pub path: String,
    /// Blake3 content hash
    pub hash: Blake3Hash,
    /// File size in bytes
    pub file_size: u64,
    /// Number of rows written
    pub row_count: i64,
    /// Arrow schema used
    pub arrow_schema: Arc<arrow::datatypes::Schema>,
    /// Parquet metadata (for Iceberg DataFile construction)
    pub parquet_metadata: Arc<parquet::file::metadata::ParquetMetaData>,
    /// Timestamp when write completed
    pub completed_at: chrono::DateTime<chrono::Utc>,
}

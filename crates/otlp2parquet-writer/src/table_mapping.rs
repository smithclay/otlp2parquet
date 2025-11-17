//! Table name mapping for OTLP signals

use otlp2parquet_core::SignalType;

/// Get the table name for a given signal type and optional metric type
///
/// Returns the canonical table name used in Iceberg catalog
#[allow(dead_code)]
pub fn table_name_for_signal(signal: SignalType, metric_type: Option<&str>) -> String {
    match signal {
        SignalType::Logs => "logs".to_string(),
        SignalType::Traces => "traces".to_string(),
        SignalType::Metrics => match metric_type {
            Some("gauge") => "metrics_gauge".to_string(),
            Some("sum") => "metrics_sum".to_string(),
            Some("histogram") => "metrics_histogram".to_string(),
            Some("exponential_histogram") => "metrics_exponential_histogram".to_string(),
            Some("summary") => "metrics_summary".to_string(),
            _ => panic!("Unknown or missing metric type: {:?}", metric_type),
        },
    }
}

/// Get the signal type string for partition paths
///
/// Returns the top-level directory name (logs, traces, or metrics)
#[allow(dead_code)]
pub fn signal_type_for_partition(signal: SignalType) -> &'static str {
    match signal {
        SignalType::Logs => "logs",
        SignalType::Traces => "traces",
        SignalType::Metrics => "metrics",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_name_for_signal() {
        assert_eq!(table_name_for_signal(SignalType::Logs, None), "logs");
        assert_eq!(table_name_for_signal(SignalType::Traces, None), "traces");
        assert_eq!(
            table_name_for_signal(SignalType::Metrics, Some("gauge")),
            "metrics_gauge"
        );
        assert_eq!(
            table_name_for_signal(SignalType::Metrics, Some("sum")),
            "metrics_sum"
        );
        assert_eq!(
            table_name_for_signal(SignalType::Metrics, Some("histogram")),
            "metrics_histogram"
        );
        assert_eq!(
            table_name_for_signal(SignalType::Metrics, Some("exponential_histogram")),
            "metrics_exponential_histogram"
        );
        assert_eq!(
            table_name_for_signal(SignalType::Metrics, Some("summary")),
            "metrics_summary"
        );
    }

    #[test]
    fn test_signal_type_for_partition() {
        assert_eq!(signal_type_for_partition(SignalType::Logs), "logs");
        assert_eq!(signal_type_for_partition(SignalType::Traces), "traces");
        assert_eq!(signal_type_for_partition(SignalType::Metrics), "metrics");
    }
}

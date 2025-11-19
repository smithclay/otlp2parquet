//! Table name mapping for OTLP signals

use anyhow::Result;
use otlp2parquet_core::SignalType;

/// Get the table name for a given signal type and optional metric type
///
/// Returns the canonical table name used in Iceberg catalog
/// TODO: Will be used by icepick when catalog support is added
pub fn table_name_for_signal(signal: SignalType, metric_type: Option<&str>) -> Result<String> {
    match signal {
        SignalType::Logs => Ok("otel_logs".to_string()),
        SignalType::Traces => Ok("otel_traces".to_string()),
        SignalType::Metrics => match metric_type {
            Some("gauge") => Ok("otel_metrics_gauge".to_string()),
            Some("sum") => Ok("otel_metrics_sum".to_string()),
            Some("histogram") => Ok("otel_metrics_histogram".to_string()),
            Some("exponential_histogram") => Ok("otel_metrics_exponential_histogram".to_string()),
            Some("summary") => Ok("otel_metrics_summary".to_string()),
            _ => Err(anyhow::anyhow!(
                "Unknown or missing metric type: {:?}",
                metric_type
            )),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_name_for_signal() {
        assert_eq!(
            table_name_for_signal(SignalType::Logs, None).unwrap(),
            "otel_logs"
        );
        assert_eq!(
            table_name_for_signal(SignalType::Traces, None).unwrap(),
            "otel_traces"
        );
        assert_eq!(
            table_name_for_signal(SignalType::Metrics, Some("gauge")).unwrap(),
            "otel_metrics_gauge"
        );
        assert_eq!(
            table_name_for_signal(SignalType::Metrics, Some("sum")).unwrap(),
            "otel_metrics_sum"
        );
        assert_eq!(
            table_name_for_signal(SignalType::Metrics, Some("histogram")).unwrap(),
            "otel_metrics_histogram"
        );
        assert_eq!(
            table_name_for_signal(SignalType::Metrics, Some("exponential_histogram")).unwrap(),
            "otel_metrics_exponential_histogram"
        );
        assert_eq!(
            table_name_for_signal(SignalType::Metrics, Some("summary")).unwrap(),
            "otel_metrics_summary"
        );
    }

    #[test]
    fn test_table_name_for_signal_error() {
        // Test that unknown metric type returns error instead of panicking
        assert!(table_name_for_signal(SignalType::Metrics, None).is_err());
        assert!(table_name_for_signal(SignalType::Metrics, Some("unknown")).is_err());
    }
}

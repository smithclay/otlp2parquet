/// Result of processing a signal request
#[derive(Debug, Clone, PartialEq)]
pub struct ProcessingResult {
    pub paths_written: Vec<String>,
    pub records_processed: usize,
    pub batches_flushed: usize,
}

use crate::error::OtlpError;
use otlp2parquet_batch::{LogSignalProcessor, PassthroughBatcher};
use otlp2parquet_common::{
    count_skipped_metric_data_points, normalise_json_value, normalize_json_bytes, InputFormat,
    MetricType, SignalType,
};
use otlp2parquet_writer::WriteBatchRequest;
use otlp2records::{
    apply_log_transform, apply_metric_transform, apply_trace_transform, decode_logs,
    decode_metrics, decode_traces, gauge_schema, sum_schema, traces_schema, DecodeMetricsResult,
    InputFormat as RecordsFormat, SkippedMetrics,
};
use std::collections::HashMap;
use vrl::value::{KeyString, Value};

/// Process OTLP logs request
pub async fn process_logs(body: &[u8], format: InputFormat) -> Result<ProcessingResult, OtlpError> {
    let values = decode_logs_values(body, format)?;
    let transformed = apply_log_transform(values).map_err(|e| OtlpError::ConversionFailed {
        signal: "logs".into(),
        message: e.to_string(),
    })?;

    let per_service_values = group_values_by_service(transformed);
    let passthrough = PassthroughBatcher::<LogSignalProcessor>::default();
    let mut batches = Vec::new();
    let mut total_records = 0;

    for (_, subset) in per_service_values {
        let batch = passthrough
            .ingest(&subset)
            .map_err(|e| OtlpError::ConversionFailed {
                signal: "logs".into(),
                message: e.to_string(),
            })?;
        total_records += batch.metadata.record_count;
        batches.push(batch);
    }

    let mut paths = Vec::new();
    let batch_count = batches.len();
    for batch in batches {
        for record_batch in &batch.batches {
            let path = otlp2parquet_writer::write_batch(WriteBatchRequest {
                batch: record_batch,
                signal_type: SignalType::Logs,
                metric_type: None,
                service_name: &batch.metadata.service_name,
                timestamp_micros: batch.metadata.first_timestamp_micros,
            })
            .await
            .map_err(|e| OtlpError::StorageFailed {
                message: e.to_string(),
            })?;

            paths.push(path);
        }
    }

    Ok(ProcessingResult {
        paths_written: paths,
        records_processed: total_records,
        batches_flushed: batch_count,
    })
}

/// Process OTLP traces request
pub async fn process_traces(
    body: &[u8],
    format: InputFormat,
) -> Result<ProcessingResult, OtlpError> {
    let values = decode_traces_values(body, format)?;
    let transformed = apply_trace_transform(values).map_err(|e| OtlpError::ConversionFailed {
        signal: "traces".into(),
        message: e.to_string(),
    })?;

    let per_service_values = group_values_by_service(transformed);
    let mut paths = Vec::new();
    let mut spans_processed = 0;

    for (service_name, subset) in per_service_values {
        let batch = otlp2records::values_to_arrow(&subset, &traces_schema()).map_err(|e| {
            OtlpError::ConversionFailed {
                signal: "traces".into(),
                message: e.to_string(),
            }
        })?;

        if batch.num_rows() == 0 {
            continue;
        }

        let first_timestamp_micros = first_timestamp_micros(&subset);
        spans_processed += subset.len();

        let path = otlp2parquet_writer::write_batch(WriteBatchRequest {
            batch: &batch,
            signal_type: SignalType::Traces,
            metric_type: None,
            service_name: &service_name,
            timestamp_micros: first_timestamp_micros,
        })
        .await
        .map_err(|e| OtlpError::StorageFailed {
            message: e.to_string(),
        })?;

        paths.push(path);
    }

    let batch_count = paths.len();
    Ok(ProcessingResult {
        paths_written: paths,
        records_processed: spans_processed,
        batches_flushed: batch_count,
    })
}

/// Process OTLP metrics request
pub async fn process_metrics(
    body: &[u8],
    format: InputFormat,
) -> Result<ProcessingResult, OtlpError> {
    let decode_result = decode_metrics_values(body, format)?;
    report_skipped_metrics(&decode_result.skipped);

    let metric_values =
        apply_metric_transform(decode_result.values).map_err(|e| OtlpError::ConversionFailed {
            signal: "metrics".into(),
            message: e.to_string(),
        })?;

    let mut paths = Vec::new();
    let mut total_data_points = 0;

    total_data_points +=
        write_metric_batches(MetricType::Gauge, &metric_values.gauge, &mut paths).await?;

    total_data_points +=
        write_metric_batches(MetricType::Sum, &metric_values.sum, &mut paths).await?;

    let batch_count = paths.len();
    Ok(ProcessingResult {
        paths_written: paths,
        records_processed: total_data_points,
        batches_flushed: batch_count,
    })
}

async fn write_metric_batches(
    metric_type: MetricType,
    values: &[Value],
    paths: &mut Vec<String>,
) -> Result<usize, OtlpError> {
    if values.is_empty() {
        return Ok(0);
    }

    let schema = match metric_type {
        MetricType::Gauge => gauge_schema(),
        MetricType::Sum => sum_schema(),
        _ => {
            return Ok(0);
        }
    };

    let mut total_data_points = 0;

    for (service_name, subset) in group_values_by_service(values.to_vec()) {
        let batch = otlp2records::values_to_arrow(&subset, &schema).map_err(|e| {
            OtlpError::ConversionFailed {
                signal: "metrics".into(),
                message: e.to_string(),
            }
        })?;

        if batch.num_rows() == 0 {
            continue;
        }

        let first_timestamp_micros = first_timestamp_micros(&subset);
        total_data_points += subset.len();

        let path = otlp2parquet_writer::write_batch(WriteBatchRequest {
            batch: &batch,
            signal_type: SignalType::Metrics,
            metric_type: Some(metric_type.as_str()),
            service_name: &service_name,
            timestamp_micros: first_timestamp_micros,
        })
        .await
        .map_err(|e| OtlpError::StorageFailed {
            message: e.to_string(),
        })?;

        paths.push(path);
    }

    Ok(total_data_points)
}

fn decode_logs_values(body: &[u8], format: InputFormat) -> Result<Vec<Value>, OtlpError> {
    match format {
        InputFormat::Jsonl => {
            decode_jsonl_values(body, |line| decode_logs(line, RecordsFormat::Json))
                .map_err(|e| invalid_request("logs", e))
        }
        InputFormat::Json => {
            let normalized =
                normalize_json_bytes(body).map_err(|e| invalid_request("logs", e.to_string()))?;
            decode_logs(&normalized, RecordsFormat::Json)
                .map_err(|e| invalid_request("logs", e.to_string()))
        }
        InputFormat::Protobuf => decode_logs(body, to_records_format(format))
            .map_err(|e| invalid_request("logs", e.to_string())),
    }
}

fn decode_traces_values(body: &[u8], format: InputFormat) -> Result<Vec<Value>, OtlpError> {
    match format {
        InputFormat::Jsonl => {
            decode_jsonl_values(body, |line| decode_traces(line, RecordsFormat::Json))
                .map_err(|e| invalid_request("traces", e))
        }
        InputFormat::Json => {
            let normalized =
                normalize_json_bytes(body).map_err(|e| invalid_request("traces", e.to_string()))?;
            decode_traces(&normalized, RecordsFormat::Json)
                .map_err(|e| invalid_request("traces", e.to_string()))
        }
        InputFormat::Protobuf => decode_traces(body, to_records_format(format))
            .map_err(|e| invalid_request("traces", e.to_string())),
    }
}

fn decode_metrics_values(
    body: &[u8],
    format: InputFormat,
) -> Result<DecodeMetricsResult, OtlpError> {
    match format {
        InputFormat::Jsonl => decode_jsonl_metrics(body),
        InputFormat::Json => decode_metrics_json(body),
        InputFormat::Protobuf => decode_metrics(body, to_records_format(format))
            .map_err(|e| invalid_request("metrics", e.to_string())),
    }
}

fn decode_jsonl_values<F>(body: &[u8], mut decode: F) -> Result<Vec<Value>, String>
where
    F: FnMut(&[u8]) -> Result<Vec<Value>, otlp2records::decode::DecodeError>,
{
    let text = std::str::from_utf8(body).map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    let mut saw_line = false;

    for (line_num, line) in text.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        saw_line = true;
        let normalized = normalize_json_bytes(trimmed.as_bytes())
            .map_err(|e| format!("line {}: {}", line_num + 1, e))?;
        let values = decode(&normalized).map_err(|e| format!("line {}: {}", line_num + 1, e))?;
        out.extend(values);
    }

    if !saw_line {
        return Err("jsonl payload contained no records".to_string());
    }

    Ok(out)
}

fn decode_jsonl_metrics(body: &[u8]) -> Result<DecodeMetricsResult, OtlpError> {
    let text = std::str::from_utf8(body).map_err(|e| invalid_request("metrics", e.to_string()))?;
    let mut values = Vec::new();
    let mut skipped = SkippedMetrics::default();
    let mut saw_line = false;

    for (line_num, line) in text.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        saw_line = true;
        let mut value: serde_json::Value = serde_json::from_slice(trimmed.as_bytes())
            .map_err(|e| invalid_request("metrics", format!("line {}: {}", line_num + 1, e)))?;
        let counts = count_skipped_metric_data_points(&value);
        normalise_json_value(&mut value, None)
            .map_err(|e| invalid_request("metrics", format!("line {}: {}", line_num + 1, e)))?;
        let normalized = serde_json::to_vec(&value)
            .map_err(|e| invalid_request("metrics", format!("line {}: {}", line_num + 1, e)))?;
        let mut result = decode_metrics(&normalized, RecordsFormat::Json)
            .map_err(|e| invalid_request("metrics", format!("line {}: {}", line_num + 1, e)))?;
        result.skipped.histograms += counts.histograms;
        result.skipped.exponential_histograms += counts.exponential_histograms;
        result.skipped.summaries += counts.summaries;
        values.extend(result.values);
        merge_skipped(&mut skipped, &result.skipped);
    }

    if !saw_line {
        return Err(invalid_request(
            "metrics",
            "jsonl payload contained no records".to_string(),
        ));
    }

    Ok(DecodeMetricsResult { values, skipped })
}

fn decode_metrics_json(body: &[u8]) -> Result<DecodeMetricsResult, OtlpError> {
    let mut value: serde_json::Value =
        serde_json::from_slice(body).map_err(|e| invalid_request("metrics", e.to_string()))?;
    let counts = count_skipped_metric_data_points(&value);
    normalise_json_value(&mut value, None)
        .map_err(|e| invalid_request("metrics", e.to_string()))?;
    let normalized =
        serde_json::to_vec(&value).map_err(|e| invalid_request("metrics", e.to_string()))?;
    let mut result = decode_metrics(&normalized, RecordsFormat::Json)
        .map_err(|e| invalid_request("metrics", e.to_string()))?;
    result.skipped.histograms += counts.histograms;
    result.skipped.exponential_histograms += counts.exponential_histograms;
    result.skipped.summaries += counts.summaries;
    Ok(result)
}

fn merge_skipped(target: &mut SkippedMetrics, other: &SkippedMetrics) {
    target.histograms += other.histograms;
    target.exponential_histograms += other.exponential_histograms;
    target.summaries += other.summaries;
    target.nan_values += other.nan_values;
    target.infinity_values += other.infinity_values;
    target.missing_values += other.missing_values;
}

fn report_skipped_metrics(skipped: &SkippedMetrics) {
    if skipped.has_skipped() {
        tracing::debug!(
            histograms = skipped.histograms,
            exponential_histograms = skipped.exponential_histograms,
            summaries = skipped.summaries,
            nan_values = skipped.nan_values,
            infinity_values = skipped.infinity_values,
            missing_values = skipped.missing_values,
            total = skipped.total(),
            "Skipped unsupported or invalid metric data points"
        );
    }
}

fn invalid_request(signal: &str, message: impl Into<String>) -> OtlpError {
    OtlpError::InvalidRequest {
        message: format!(
            "Failed to parse OTLP {} request: {}",
            signal,
            message.into()
        ),
        hint: Some(
            "Ensure the request body contains valid OTLP protobuf, JSON, or JSONL format.".into(),
        ),
    }
}

fn to_records_format(format: InputFormat) -> RecordsFormat {
    match format {
        InputFormat::Protobuf => RecordsFormat::Protobuf,
        InputFormat::Json => RecordsFormat::Json,
        InputFormat::Jsonl => RecordsFormat::Json,
    }
}

fn group_values_by_service(values: Vec<Value>) -> Vec<(String, Vec<Value>)> {
    let mut order = Vec::new();
    let mut index: HashMap<String, usize> = HashMap::new();
    let mut groups: Vec<Vec<Value>> = Vec::new();

    for value in values {
        let service = extract_service_name(&value);
        let idx = if let Some(idx) = index.get(&service) {
            *idx
        } else {
            let idx = groups.len();
            index.insert(service.clone(), idx);
            order.push(service);
            groups.push(Vec::new());
            idx
        };
        groups[idx].push(value);
    }

    order.into_iter().zip(groups).collect()
}

fn extract_service_name(value: &Value) -> String {
    let key: KeyString = "service_name".into();
    if let Value::Object(map) = value {
        if let Some(Value::Bytes(bytes)) = map.get(&key) {
            if !bytes.is_empty() {
                return String::from_utf8_lossy(bytes).into_owned();
            }
        }
    }
    "unknown".to_string()
}

fn first_timestamp_micros(values: &[Value]) -> i64 {
    let key: KeyString = "timestamp".into();
    let mut min: Option<i64> = None;

    for value in values {
        if let Value::Object(map) = value {
            if let Some(ts) = map.get(&key) {
                if let Some(millis) = value_to_i64(ts) {
                    let micros = millis.saturating_mul(1_000);
                    min = Some(min.map_or(micros, |current| current.min(micros)));
                }
            }
        }
    }

    min.unwrap_or(0)
}

fn value_to_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Integer(i) => Some(*i),
        Value::Float(f) => Some(f.into_inner() as i64),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_processing_result_creation() {
        let result = ProcessingResult {
            paths_written: vec!["path1".to_string(), "path2".to_string()],
            records_processed: 100,
            batches_flushed: 2,
        };

        assert_eq!(result.paths_written.len(), 2);
        assert_eq!(result.records_processed, 100);
        assert_eq!(result.batches_flushed, 2);
    }

    #[tokio::test]
    async fn test_process_logs_invalid_request() {
        let invalid_data = b"not valid otlp data";

        let result = process_logs(invalid_data, InputFormat::Protobuf).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.error_type(), "InvalidRequest");
        assert!(err.message().contains("Failed to parse OTLP logs request"));
    }

    #[test]
    fn test_group_values_by_service_preserves_order() {
        let values = vec![
            make_value("service-a"),
            make_value("service-b"),
            make_value("service-a"),
        ];

        let grouped = group_values_by_service(values);
        assert_eq!(grouped.len(), 2);
        assert_eq!(grouped[0].0, "service-a");
        assert_eq!(grouped[1].0, "service-b");
        assert_eq!(grouped[0].1.len(), 2);
        assert_eq!(grouped[1].1.len(), 1);
    }

    fn make_value(service_name: &str) -> Value {
        let mut map = vrl::value::ObjectMap::new();
        map.insert(
            "service_name".into(),
            Value::Bytes(bytes::Bytes::copy_from_slice(service_name.as_bytes())),
        );
        map.insert("timestamp".into(), Value::Integer(1_700_000_000));
        map.insert("observed_timestamp".into(), Value::Integer(1_700_000_000));
        map.insert("severity_number".into(), Value::Integer(9));
        map.insert(
            "severity_text".into(),
            Value::Bytes(bytes::Bytes::copy_from_slice("INFO".as_bytes())),
        );
        Value::Object(map)
    }
}

// Synthetic OTLP log data generators for benchmarking
//
// Creates realistic ExportLogsServiceRequest instances with configurable:
// - Number of log records (10k, 250k, 1M)
// - Format (protobuf, JSON)
// - Compression (gzip, none)

use otlp2parquet_proto::opentelemetry::proto::{
    collector::logs::v1::ExportLogsServiceRequest,
    common::v1::{any_value, AnyValue, KeyValue},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    resource::v1::Resource,
};
use prost::Message;
use std::time::{SystemTime, UNIX_EPOCH};

/// Workload size presets
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
pub enum WorkloadSize {
    Small,  // 10k log records
    Medium, // 250k log records
    Large,  // 1M log records
}

impl WorkloadSize {
    pub fn record_count(&self) -> usize {
        match self {
            WorkloadSize::Small => 10_000,
            WorkloadSize::Medium => 250_000,
            WorkloadSize::Large => 1_000_000,
        }
    }
}

/// Format for serialized data
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
pub enum Format {
    Protobuf,
    Json,
}

/// Compression option
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
pub enum Compression {
    None,
    Gzip,
}

/// Generate a synthetic OTLP ExportLogsServiceRequest
pub fn generate_otlp_logs(size: WorkloadSize) -> ExportLogsServiceRequest {
    let record_count = size.record_count();
    let now_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;

    // Group logs into resource batches (simulate realistic grouping)
    // Each resource represents a different service instance
    let num_resources = (record_count / 1000).clamp(1, 100);
    let records_per_resource = record_count / num_resources;

    let resource_logs: Vec<ResourceLogs> = (0..num_resources)
        .map(|resource_idx| {
            let service_name = format!("bench-service-{}", resource_idx % 10);
            let service_namespace = "benchmark";
            let service_instance_id = format!("instance-{}", resource_idx);

            ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![
                        key_value("service.name", &service_name),
                        key_value("service.namespace", service_namespace),
                        key_value("service.instance.id", &service_instance_id),
                        key_value("host.name", &format!("host-{}", resource_idx % 20)),
                        key_value("deployment.environment", "benchmark"),
                    ],
                    dropped_attributes_count: 0,
                }),
                scope_logs: vec![ScopeLogs {
                    scope: Some(otlp2parquet_proto::opentelemetry::proto::common::v1::InstrumentationScope {
                        name: "benchmark-logger".to_string(),
                        version: "1.0.0".to_string(),
                        attributes: vec![],
                        dropped_attributes_count: 0,
                    }),
                    log_records: (0..records_per_resource)
                        .map(|log_idx| {
                            let global_idx = resource_idx * records_per_resource + log_idx;
                            generate_log_record(global_idx, now_nanos)
                        })
                        .collect(),
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }
        })
        .collect();

    ExportLogsServiceRequest { resource_logs }
}

/// Generate a single realistic log record
fn generate_log_record(idx: usize, base_timestamp_nanos: u64) -> LogRecord {
    // Vary timestamps slightly to simulate realistic time distribution
    let timestamp_nanos = base_timestamp_nanos - (idx as u64 * 1_000_000); // 1ms apart
    let observed_timestamp_nanos = timestamp_nanos + 100_000; // 100μs observation delay

    // Generate trace/span IDs for about 50% of logs (realistic for traced logs)
    let (trace_id, span_id, trace_flags) = if idx.is_multiple_of(2) {
        let trace_id = generate_trace_id(idx);
        let span_id = generate_span_id(idx);
        (trace_id, span_id, 1u32) // Sampled
    } else {
        (vec![], vec![], 0u32)
    };

    // Vary severity to simulate realistic distribution
    let (severity_number, severity_text) = match idx % 5 {
        0 => (5, "DEBUG"),
        1 => (9, "INFO"),
        2 => (13, "WARN"),
        3 => (17, "ERROR"),
        _ => (21, "FATAL"),
    };

    LogRecord {
        time_unix_nano: timestamp_nanos,
        observed_time_unix_nano: observed_timestamp_nanos,
        severity_number,
        severity_text: severity_text.to_string(),
        body: Some(AnyValue {
            value: Some(any_value::Value::StringValue(format!(
                "Benchmark log message #{} with some realistic content and details",
                idx
            ))),
        }),
        attributes: vec![
            key_value("log.level", severity_text),
            key_value("thread.id", &(idx % 100).to_string()),
            key_value("file.name", "benchmark.rs"),
            key_value("code.lineno", &((idx % 1000) + 1).to_string()),
            key_value_int("request.id", (idx / 10) as i64),
            key_value_bool("is_sampled", idx.is_multiple_of(2)),
        ],
        dropped_attributes_count: 0,
        flags: trace_flags,
        trace_id,
        span_id,
    }
}

/// Helper to create a string KeyValue
fn key_value(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.to_string())),
        }),
    }
}

/// Helper to create an integer KeyValue
fn key_value_int(key: &str, value: i64) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::IntValue(value)),
        }),
    }
}

/// Helper to create a boolean KeyValue
fn key_value_bool(key: &str, value: bool) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::BoolValue(value)),
        }),
    }
}

/// Generate a realistic 16-byte trace ID
fn generate_trace_id(idx: usize) -> Vec<u8> {
    let mut trace_id = vec![0u8; 16];
    let idx_bytes = (idx as u64).to_be_bytes();
    trace_id[8..].copy_from_slice(&idx_bytes);
    trace_id
}

/// Generate a realistic 8-byte span ID
fn generate_span_id(idx: usize) -> Vec<u8> {
    (idx as u64).to_be_bytes().to_vec()
}

/// Serialize request to protobuf bytes
#[allow(dead_code)]
pub fn to_protobuf(request: &ExportLogsServiceRequest) -> Vec<u8> {
    request.encode_to_vec()
}

/// Serialize request to JSON bytes
#[allow(dead_code)]
pub fn to_json(request: &ExportLogsServiceRequest) -> Vec<u8> {
    serde_json::to_vec(request).expect("Failed to serialize to JSON")
}

/// Compress bytes with gzip
#[allow(dead_code)]
pub fn compress_gzip(data: &[u8]) -> Vec<u8> {
    use flate2::write::GzEncoder;
    use flate2::Compression as GzipLevel;
    use std::io::Write;

    let mut encoder = GzEncoder::new(Vec::new(), GzipLevel::default());
    encoder
        .write_all(data)
        .expect("Failed to write to gzip encoder");
    encoder.finish().expect("Failed to finish gzip encoding")
}

/// Get serialized data in the specified format and compression
#[allow(dead_code)]
pub fn get_serialized_data(
    request: &ExportLogsServiceRequest,
    format: Format,
    compression: Compression,
) -> Vec<u8> {
    let data = match format {
        Format::Protobuf => to_protobuf(request),
        Format::Json => to_json(request),
    };

    match compression {
        Compression::None => data,
        Compression::Gzip => compress_gzip(&data),
    }
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::*;

    #[test]
    fn test_generate_small_workload() {
        let request = generate_otlp_logs(WorkloadSize::Small);
        let total_records: usize = request
            .resource_logs
            .iter()
            .flat_map(|rl| &rl.scope_logs)
            .map(|sl| sl.log_records.len())
            .sum();
        assert_eq!(total_records, 10_000);
    }

    #[test]
    fn test_serialization() {
        let request = generate_otlp_logs(WorkloadSize::Small);

        // Test protobuf serialization
        let proto_bytes = to_protobuf(&request);
        assert!(!proto_bytes.is_empty());

        // Test JSON serialization
        let json_bytes = to_json(&request);
        assert!(!json_bytes.is_empty());

        // Test gzip compression
        let compressed = compress_gzip(&proto_bytes);
        assert!(compressed.len() < proto_bytes.len());
    }
}

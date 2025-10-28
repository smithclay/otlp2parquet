// Integration tests for otlp2parquet-core
//
// Tests the complete end-to-end workflow from OTLP bytes to Parquet

use otlp2parquet_core::process_otlp_logs;
use otlp2parquet_proto::opentelemetry::proto::{
    collector::logs::v1::ExportLogsServiceRequest,
    common::v1::{any_value, AnyValue, KeyValue},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    resource::v1::Resource,
};
use prost::Message;

/// Create a sample OTLP log request
fn create_sample_otlp_request() -> Vec<u8> {
    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![
                    KeyValue {
                        key: "service.name".to_string(),
                        value: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("test-service".to_string())),
                        }),
                    },
                    KeyValue {
                        key: "service.namespace".to_string(),
                        value: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("production".to_string())),
                        }),
                    },
                    KeyValue {
                        key: "host.name".to_string(),
                        value: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("server-001".to_string())),
                        }),
                    },
                ],
                dropped_attributes_count: 0,
            }),
            scope_logs: vec![ScopeLogs {
                scope: Some(
                    otlp2parquet_proto::opentelemetry::proto::common::v1::InstrumentationScope {
                        name: "test-logger".to_string(),
                        version: "1.0.0".to_string(),
                        attributes: vec![],
                        dropped_attributes_count: 0,
                    },
                ),
                log_records: vec![
                    LogRecord {
                        time_unix_nano: 1705327800000000000, // 2024-01-15 14:30:00 UTC
                        observed_time_unix_nano: 1705327800000000000,
                        severity_number: 9, // INFO
                        severity_text: "INFO".to_string(),
                        body: Some(AnyValue {
                            value: Some(any_value::Value::StringValue(
                                "Test log message".to_string(),
                            )),
                        }),
                        attributes: vec![KeyValue {
                            key: "http.method".to_string(),
                            value: Some(AnyValue {
                                value: Some(any_value::Value::StringValue("GET".to_string())),
                            }),
                        }],
                        dropped_attributes_count: 0,
                        flags: 1,
                        trace_id: vec![1u8; 16],
                        span_id: vec![2u8; 8],
                    },
                    LogRecord {
                        time_unix_nano: 1705327801000000000, // 1 second later
                        observed_time_unix_nano: 1705327801000000000,
                        severity_number: 13, // ERROR
                        severity_text: "ERROR".to_string(),
                        body: Some(AnyValue {
                            value: Some(any_value::Value::StringValue(
                                "Error log message".to_string(),
                            )),
                        }),
                        attributes: vec![],
                        dropped_attributes_count: 0,
                        flags: 1,
                        trace_id: vec![3u8; 16],
                        span_id: vec![4u8; 8],
                    },
                ],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };

    let mut bytes = Vec::new();
    request.encode(&mut bytes).unwrap();
    bytes
}

#[test]
fn test_end_to_end_processing() {
    // Create sample OTLP request
    let otlp_bytes = create_sample_otlp_request();

    // Process logs
    let result = process_otlp_logs(&otlp_bytes);
    assert!(result.is_ok(), "Processing failed: {:?}", result.err());

    let processing_result = result.unwrap();

    // Verify parquet bytes
    assert!(
        !processing_result.parquet_bytes.is_empty(),
        "Parquet bytes should not be empty"
    );
    assert_eq!(
        &processing_result.parquet_bytes[0..4],
        b"PAR1",
        "Parquet should start with magic bytes"
    );

    // Verify metadata
    assert_eq!(
        processing_result.service_name, "test-service",
        "Service name should be extracted"
    );
    assert_eq!(
        processing_result.timestamp_nanos, 1705327800000000000,
        "Timestamp should be extracted"
    );
}

#[test]
fn test_empty_logs_handling() {
    let request = ExportLogsServiceRequest {
        resource_logs: vec![],
    };

    let mut bytes = Vec::new();
    request.encode(&mut bytes).unwrap();

    let result = process_otlp_logs(&bytes);
    assert!(result.is_ok(), "Empty logs should be handled");

    let processing_result = result.unwrap();
    assert!(!processing_result.parquet_bytes.is_empty());
    assert_eq!(processing_result.service_name, "unknown");
}

#[test]
fn test_parquet_structure() {
    // Create sample OTLP request
    let otlp_bytes = create_sample_otlp_request();

    // Process logs
    let processing_result = process_otlp_logs(&otlp_bytes).unwrap();

    // Verify Parquet structure by reading it back
    use arrow::array::{Array, RecordBatch, StringArray, TimestampNanosecondArray};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::io::Cursor;

    let cursor = Cursor::new(processing_result.parquet_bytes);
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(cursor).expect("Failed to create Parquet reader");

    let mut reader = builder.build().expect("Failed to build reader");
    let batch = reader
        .next()
        .expect("Should have at least one batch")
        .expect("Failed to read batch");

    // Verify schema
    assert_eq!(batch.num_columns(), 15, "Should have 15 columns");
    assert_eq!(batch.num_rows(), 2, "Should have 2 log records");

    // Verify service name column
    let service_name_col = batch
        .column(8)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Column 8 should be StringArray");
    assert_eq!(service_name_col.value(0), "test-service");
    assert_eq!(service_name_col.value(1), "test-service");

    // Verify timestamp column
    let timestamp_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .expect("Column 0 should be TimestampNanosecondArray");
    assert_eq!(timestamp_col.value(0), 1705327800000000000);
    assert_eq!(timestamp_col.value(1), 1705327801000000000);

    // Verify body column
    let body_col = batch
        .column(7)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Column 7 should be StringArray");
    assert_eq!(body_col.value(0), "Test log message");
    assert_eq!(body_col.value(1), "Error log message");
}

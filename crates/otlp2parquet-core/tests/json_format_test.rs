// Integration tests for JSON and JSONL format support
//
// Tests the format-aware processing workflow that accepts JSON and JSONL
// in addition to the native protobuf format.

use otlp2parquet_core::{process_otlp_logs_with_format, InputFormat};
use otlp2parquet_proto::opentelemetry::proto::{
    collector::logs::v1::ExportLogsServiceRequest,
    common::v1::{any_value, AnyValue, KeyValue},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    resource::v1::Resource,
};
use prost::Message;

/// Create a sample OTLP log request (same as integration_test.rs)
fn create_sample_otlp_request() -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
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
    }
}

#[test]
fn test_json_format_processing() {
    // Create sample OTLP request
    let request = create_sample_otlp_request();

    // Serialize to JSON
    let json_bytes = serde_json::to_vec(&request).expect("Failed to serialize to JSON");

    // Process as JSON
    let result = process_otlp_logs_with_format(&json_bytes, InputFormat::Json);
    assert!(result.is_ok(), "JSON processing failed: {:?}", result.err());

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
        "Service name should be extracted from JSON"
    );
    assert_eq!(
        processing_result.timestamp_nanos, 1705327800000000000,
        "Timestamp should be extracted from JSON"
    );
}

#[test]
fn test_protobuf_and_json_produce_same_output() {
    // Create sample OTLP request
    let request = create_sample_otlp_request();

    // Process as protobuf
    let mut protobuf_bytes = Vec::new();
    request.encode(&mut protobuf_bytes).unwrap();
    let protobuf_result = process_otlp_logs_with_format(&protobuf_bytes, InputFormat::Protobuf)
        .expect("Protobuf processing failed");

    // Process as JSON
    let json_bytes = serde_json::to_vec(&request).expect("Failed to serialize to JSON");
    let json_result = process_otlp_logs_with_format(&json_bytes, InputFormat::Json)
        .expect("JSON processing failed");

    // Metadata should match
    assert_eq!(
        protobuf_result.service_name, json_result.service_name,
        "Service names should match"
    );
    assert_eq!(
        protobuf_result.timestamp_nanos, json_result.timestamp_nanos,
        "Timestamps should match"
    );

    // Both should produce valid Parquet
    assert_eq!(&protobuf_result.parquet_bytes[0..4], b"PAR1");
    assert_eq!(&json_result.parquet_bytes[0..4], b"PAR1");

    // Parquet files should have same structure (read back and compare)
    use arrow::array::{Array, StringArray};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let protobuf_batch = {
        let bytes = prost::bytes::Bytes::from(protobuf_result.parquet_bytes);
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .unwrap()
            .build()
            .unwrap();
        reader.next().unwrap().unwrap()
    };

    let json_batch = {
        let bytes = prost::bytes::Bytes::from(json_result.parquet_bytes);
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .unwrap()
            .build()
            .unwrap();
        reader.next().unwrap().unwrap()
    };

    // Same schema
    assert_eq!(protobuf_batch.num_columns(), json_batch.num_columns());
    assert_eq!(protobuf_batch.num_rows(), json_batch.num_rows());

    // Same data in body column (column 7)
    let protobuf_body = protobuf_batch
        .column(7)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let json_body = json_batch
        .column(7)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    assert_eq!(protobuf_body.value(0), json_body.value(0));
    assert_eq!(protobuf_body.value(1), json_body.value(1));
}

#[test]
fn test_jsonl_format_processing() {
    // Create two separate OTLP requests
    let request1 = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("service-1".to_string())),
                    }),
                }],
                dropped_attributes_count: 0,
            }),
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records: vec![LogRecord {
                    time_unix_nano: 1705327800000000000,
                    observed_time_unix_nano: 1705327800000000000,
                    severity_number: 9,
                    severity_text: "INFO".to_string(),
                    body: Some(AnyValue {
                        value: Some(any_value::Value::StringValue(
                            "Log from service-1".to_string(),
                        )),
                    }),
                    attributes: vec![],
                    dropped_attributes_count: 0,
                    flags: 0,
                    trace_id: vec![],
                    span_id: vec![],
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };

    let request2 = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("service-2".to_string())),
                    }),
                }],
                dropped_attributes_count: 0,
            }),
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records: vec![LogRecord {
                    time_unix_nano: 1705327801000000000,
                    observed_time_unix_nano: 1705327801000000000,
                    severity_number: 13,
                    severity_text: "ERROR".to_string(),
                    body: Some(AnyValue {
                        value: Some(any_value::Value::StringValue(
                            "Log from service-2".to_string(),
                        )),
                    }),
                    attributes: vec![],
                    dropped_attributes_count: 0,
                    flags: 0,
                    trace_id: vec![],
                    span_id: vec![],
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };

    // Serialize to JSONL (newline-delimited JSON)
    let json1 = serde_json::to_string(&request1).unwrap();
    let json2 = serde_json::to_string(&request2).unwrap();
    let jsonl = format!("{}\n{}", json1, json2);

    // Process as JSONL
    let result = process_otlp_logs_with_format(jsonl.as_bytes(), InputFormat::Jsonl);
    assert!(
        result.is_ok(),
        "JSONL processing failed: {:?}",
        result.err()
    );

    let processing_result = result.unwrap();

    // Should produce valid Parquet
    assert_eq!(&processing_result.parquet_bytes[0..4], b"PAR1");

    // Verify that both services' logs were merged
    use arrow::array::StringArray;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let bytes = prost::bytes::Bytes::from(processing_result.parquet_bytes);
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .unwrap()
        .build()
        .unwrap();
    let batch = reader.next().unwrap().unwrap();

    // Should have 2 log records (1 from each request)
    assert_eq!(batch.num_rows(), 2);

    // Check service names (column 8)
    let service_name_col = batch
        .column(8)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(service_name_col.value(0), "service-1");
    assert_eq!(service_name_col.value(1), "service-2");

    // Check body messages (column 7)
    let body_col = batch
        .column(7)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(body_col.value(0), "Log from service-1");
    assert_eq!(body_col.value(1), "Log from service-2");
}

#[test]
fn test_jsonl_with_empty_lines() {
    // JSONL with empty lines should be handled gracefully
    let request = create_sample_otlp_request();
    let json = serde_json::to_string(&request).unwrap();

    // Add empty lines
    let jsonl = format!("\n{}\n\n  \n", json);

    let result = process_otlp_logs_with_format(jsonl.as_bytes(), InputFormat::Jsonl);
    assert!(result.is_ok(), "JSONL with empty lines should work");

    let processing_result = result.unwrap();
    assert_eq!(&processing_result.parquet_bytes[0..4], b"PAR1");
}

#[test]
fn test_jsonl_empty_input_fails() {
    // Empty JSONL should return an error
    let result = process_otlp_logs_with_format(b"", InputFormat::Jsonl);
    assert!(result.is_err(), "Empty JSONL should fail");
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("no valid log records"));

    // Only whitespace should also fail
    let result = process_otlp_logs_with_format(b"\n  \n  \n", InputFormat::Jsonl);
    assert!(result.is_err(), "Whitespace-only JSONL should fail");
}

#[test]
fn test_invalid_json_fails() {
    // Invalid JSON should return an error
    let invalid_json = b"{invalid json}";
    let result = process_otlp_logs_with_format(invalid_json, InputFormat::Json);
    assert!(result.is_err(), "Invalid JSON should fail");
}

#[test]
fn test_jsonl_invalid_line_fails() {
    // JSONL with an invalid line should fail
    let request = create_sample_otlp_request();
    let json = serde_json::to_string(&request).unwrap();
    let jsonl = format!("{}\n{{invalid}}\n{}", json, json);

    let result = process_otlp_logs_with_format(jsonl.as_bytes(), InputFormat::Jsonl);
    assert!(result.is_err(), "JSONL with invalid line should fail");
}

#[test]
fn test_format_auto_detection() {
    // Test that InputFormat::from_content_type correctly detects formats
    assert_eq!(
        InputFormat::from_content_type(Some("application/json")),
        InputFormat::Json
    );
    assert_eq!(
        InputFormat::from_content_type(Some("application/x-ndjson")),
        InputFormat::Jsonl
    );
    assert_eq!(
        InputFormat::from_content_type(Some("application/jsonl")),
        InputFormat::Jsonl
    );
    assert_eq!(
        InputFormat::from_content_type(Some("application/x-protobuf")),
        InputFormat::Protobuf
    );
    assert_eq!(
        InputFormat::from_content_type(Some("application/protobuf")),
        InputFormat::Protobuf
    );

    // Default to protobuf for unknown types
    assert_eq!(
        InputFormat::from_content_type(Some("text/plain")),
        InputFormat::Protobuf
    );
    assert_eq!(InputFormat::from_content_type(None), InputFormat::Protobuf);
}

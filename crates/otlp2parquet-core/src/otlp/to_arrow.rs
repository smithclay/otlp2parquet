// Convert OTLP log records to Arrow RecordBatch
//
// This is the core processing logic that extracts data from OTLP protobuf
// messages and builds Arrow columns according to the ClickHouse schema.

use anyhow::{Context, Result};
use arrow::array::{
    builder::MapFieldNames, FixedSizeBinaryBuilder, Int32Builder, MapBuilder, RecordBatch,
    StringBuilder, TimestampNanosecondBuilder, UInt32Builder,
};
use otlp2parquet_proto::opentelemetry::proto::{
    collector::logs::v1::ExportLogsServiceRequest,
    common::v1::{any_value, AnyValue},
};
use prost::Message;
use std::sync::Arc;

use crate::schema::{otel_logs_schema_arc, EXTRACTED_RESOURCE_ATTRS};

/// Size of OpenTelemetry TraceId in bytes (128 bits)
const TRACE_ID_SIZE: i32 = 16;

/// Size of OpenTelemetry SpanId in bytes (64 bits)
const SPAN_ID_SIZE: i32 = 8;

/// Field names for Map types in Arrow schema
fn map_field_names() -> MapFieldNames {
    MapFieldNames {
        entry: "entries".to_string(),
        key: "key".to_string(),
        value: "value".to_string(),
    }
}

/// Metadata extracted during OTLP parsing
#[derive(Debug, Clone)]
pub struct LogMetadata {
    pub service_name: String,
    pub first_timestamp_nanos: i64,
}

/// Converts OTLP log records to Arrow RecordBatch
pub struct ArrowConverter {
    // Column builders
    timestamp_builder: TimestampNanosecondBuilder,
    observed_timestamp_builder: TimestampNanosecondBuilder,
    trace_id_builder: FixedSizeBinaryBuilder,
    span_id_builder: FixedSizeBinaryBuilder,
    trace_flags_builder: UInt32Builder,
    severity_text_builder: StringBuilder,
    severity_number_builder: Int32Builder,
    body_builder: StringBuilder,
    service_name_builder: StringBuilder,
    service_namespace_builder: StringBuilder,
    service_instance_id_builder: StringBuilder,
    scope_name_builder: StringBuilder,
    scope_version_builder: StringBuilder,
    resource_attributes_builder: MapBuilder<StringBuilder, StringBuilder>,
    log_attributes_builder: MapBuilder<StringBuilder, StringBuilder>,

    // Metadata tracking (not part of schema)
    service_name: String,
    first_timestamp: Option<i64>,
}

impl ArrowConverter {
    pub fn new() -> Self {
        let schema = otel_logs_schema_arc();

        Self {
            timestamp_builder: TimestampNanosecondBuilder::new()
                .with_timezone("UTC")
                .with_data_type(schema.field(0).data_type().clone()),
            observed_timestamp_builder: TimestampNanosecondBuilder::new()
                .with_timezone("UTC")
                .with_data_type(schema.field(1).data_type().clone()),
            trace_id_builder: FixedSizeBinaryBuilder::new(TRACE_ID_SIZE),
            span_id_builder: FixedSizeBinaryBuilder::new(SPAN_ID_SIZE),
            trace_flags_builder: UInt32Builder::new(),
            severity_text_builder: StringBuilder::new(),
            severity_number_builder: Int32Builder::new(),
            body_builder: StringBuilder::new(),
            service_name_builder: StringBuilder::new(),
            service_namespace_builder: StringBuilder::new(),
            service_instance_id_builder: StringBuilder::new(),
            scope_name_builder: StringBuilder::new(),
            scope_version_builder: StringBuilder::new(),
            resource_attributes_builder: MapBuilder::new(
                Some(map_field_names()),
                StringBuilder::new(),
                StringBuilder::new(),
            ),
            log_attributes_builder: MapBuilder::new(
                Some(map_field_names()),
                StringBuilder::new(),
                StringBuilder::new(),
            ),
            service_name: String::new(),
            first_timestamp: None,
        }
    }

    /// Add OTLP log records from protobuf bytes
    pub fn add_from_proto_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        let request = ExportLogsServiceRequest::decode(bytes)
            .context("Failed to decode OTLP ExportLogsServiceRequest")?;

        for resource_logs in request.resource_logs {
            // Extract resource attributes
            let mut resource_attrs = if let Some(resource) = &resource_logs.resource {
                Vec::with_capacity(resource.attributes.len())
            } else {
                Vec::new()
            };
            let mut extracted_service_name = String::new();
            let mut extracted_service_namespace = None;
            let mut extracted_service_instance_id = None;

            if let Some(resource) = &resource_logs.resource {
                for attr in &resource.attributes {
                    let key = &attr.key;
                    let value = attr.value.as_ref().and_then(any_value_to_string);

                    if let Some(val) = value {
                        // Check if this is an extracted attribute
                        match key.as_str() {
                            "service.name" => {
                                extracted_service_name = val.clone();
                                // Track service name for metadata (first occurrence)
                                if self.service_name.is_empty() {
                                    self.service_name = val;
                                }
                            }
                            "service.namespace" => extracted_service_namespace = Some(val),
                            "service.instance.id" => extracted_service_instance_id = Some(val),
                            _ if !EXTRACTED_RESOURCE_ATTRS.contains(&key.as_str()) => {
                                resource_attrs.push((key.clone(), val));
                            }
                            _ => {}
                        }
                    }
                }
            }

            // Process each scope's logs
            for scope_logs in resource_logs.scope_logs {
                let scope_name = scope_logs
                    .scope
                    .as_ref()
                    .map(|s| s.name.as_str())
                    .unwrap_or("");
                let scope_version = scope_logs.scope.as_ref().and_then(|s| {
                    if s.version.is_empty() {
                        None
                    } else {
                        Some(s.version.as_str())
                    }
                });

                // Process each log record
                for log_record in scope_logs.log_records {
                    // Timestamps
                    let timestamp = log_record.time_unix_nano as i64;
                    self.timestamp_builder.append_value(timestamp);
                    self.observed_timestamp_builder
                        .append_value(log_record.observed_time_unix_nano as i64);

                    // Track first timestamp for metadata
                    if self.first_timestamp.is_none() {
                        self.first_timestamp = Some(timestamp);
                    }

                    // Trace context
                    if log_record.trace_id.len() == 16 {
                        self.trace_id_builder.append_value(&log_record.trace_id)?;
                    } else {
                        self.trace_id_builder.append_value([0u8; 16])?;
                    }

                    if log_record.span_id.len() == 8 {
                        self.span_id_builder.append_value(&log_record.span_id)?;
                    } else {
                        self.span_id_builder.append_value([0u8; 8])?;
                    }

                    self.trace_flags_builder.append_value(log_record.flags);

                    // Severity
                    self.severity_text_builder
                        .append_value(&log_record.severity_text);
                    self.severity_number_builder
                        .append_value(log_record.severity_number);

                    // Body
                    let body_str = log_record
                        .body
                        .as_ref()
                        .and_then(any_value_to_string)
                        .unwrap_or_default();
                    self.body_builder.append_value(&body_str);

                    // Resource attributes (extracted)
                    self.service_name_builder
                        .append_value(&extracted_service_name);
                    if let Some(ns) = &extracted_service_namespace {
                        self.service_namespace_builder.append_value(ns);
                    } else {
                        self.service_namespace_builder.append_null();
                    }
                    if let Some(id) = &extracted_service_instance_id {
                        self.service_instance_id_builder.append_value(id);
                    } else {
                        self.service_instance_id_builder.append_null();
                    }

                    // Scope
                    self.scope_name_builder.append_value(scope_name);
                    if let Some(ver) = scope_version {
                        self.scope_version_builder.append_value(ver);
                    } else {
                        self.scope_version_builder.append_null();
                    }

                    // Resource attributes (remaining)
                    for (key, value) in &resource_attrs {
                        self.resource_attributes_builder.keys().append_value(key);
                        self.resource_attributes_builder
                            .values()
                            .append_value(value);
                    }
                    self.resource_attributes_builder.append(true)?;

                    // Log attributes
                    for attr in &log_record.attributes {
                        if let Some(value) = attr.value.as_ref().and_then(any_value_to_string) {
                            self.log_attributes_builder.keys().append_value(&attr.key);
                            self.log_attributes_builder.values().append_value(&value);
                        }
                    }
                    self.log_attributes_builder.append(true)?;
                }
            }
        }

        Ok(())
    }

    pub fn finish(mut self) -> Result<(RecordBatch, LogMetadata)> {
        let schema = otel_logs_schema_arc();

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(self.timestamp_builder.finish()),
                Arc::new(self.observed_timestamp_builder.finish()),
                Arc::new(self.trace_id_builder.finish()),
                Arc::new(self.span_id_builder.finish()),
                Arc::new(self.trace_flags_builder.finish()),
                Arc::new(self.severity_text_builder.finish()),
                Arc::new(self.severity_number_builder.finish()),
                Arc::new(self.body_builder.finish()),
                Arc::new(self.service_name_builder.finish()),
                Arc::new(self.service_namespace_builder.finish()),
                Arc::new(self.service_instance_id_builder.finish()),
                Arc::new(self.scope_name_builder.finish()),
                Arc::new(self.scope_version_builder.finish()),
                Arc::new(self.resource_attributes_builder.finish()),
                Arc::new(self.log_attributes_builder.finish()),
            ],
        )?;

        // Build metadata from tracked values
        let metadata = LogMetadata {
            service_name: if self.service_name.is_empty() {
                "unknown".to_string()
            } else {
                self.service_name
            },
            first_timestamp_nanos: self.first_timestamp.unwrap_or(0),
        };

        Ok((batch, metadata))
    }
}

impl Default for ArrowConverter {
    fn default() -> Self {
        Self::new()
    }
}

/// Convert AnyValue to a string representation
fn any_value_to_string(any_val: &AnyValue) -> Option<String> {
    any_val.value.as_ref().map(|value| match value {
        any_value::Value::StringValue(s) => s.clone(),
        any_value::Value::BoolValue(b) => b.to_string(),
        any_value::Value::IntValue(i) => i.to_string(),
        any_value::Value::DoubleValue(d) => d.to_string(),
        any_value::Value::ArrayValue(_) => "[array]".to_string(),
        any_value::Value::KvlistValue(_) => "{object}".to_string(),
        any_value::Value::BytesValue(b) => format!("[bytes:{}]", b.len()),
    })
}

// Convert OTLP log records to Arrow RecordBatch
//
// This is the core processing logic that extracts data from OTLP protobuf
// messages and builds Arrow columns according to the ClickHouse schema.

use anyhow::{Context, Result};
use arrow::array::{
    FixedSizeBinaryBuilder, Int32Builder, MapBuilder, RecordBatch, StringBuilder, StructBuilder,
    TimestampNanosecondBuilder, UInt32Builder,
};
use otlp2parquet_proto::opentelemetry::proto::{
    collector::logs::v1::ExportLogsServiceRequest, common::v1::AnyValue,
};
use prost::Message;
use std::sync::Arc;

use crate::schema::{otel_logs_schema_arc, EXTRACTED_RESOURCE_ATTRS};

use super::any_value_builder::{any_value_string, append_any_value};
use super::builder_helpers::{
    map_field_names, new_any_value_struct_builder, SPAN_ID_SIZE, TRACE_ID_SIZE,
};

/// Metadata extracted during OTLP parsing
#[derive(Debug, Clone)]
pub struct LogMetadata {
    pub service_name: String,
    pub first_timestamp_nanos: i64,
    pub record_count: usize,
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
    body_builder: StructBuilder,
    service_name_builder: StringBuilder,
    service_namespace_builder: StringBuilder,
    service_instance_id_builder: StringBuilder,
    scope_name_builder: StringBuilder,
    scope_version_builder: StringBuilder,
    resource_attributes_builder: MapBuilder<StringBuilder, StructBuilder>,
    log_attributes_builder: MapBuilder<StringBuilder, StructBuilder>,

    // Metadata tracking (not part of schema)
    service_name: String,
    first_timestamp: Option<i64>,
    current_row_count: usize,
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
            body_builder: new_any_value_struct_builder(),
            service_name_builder: StringBuilder::new(),
            service_namespace_builder: StringBuilder::new(),
            service_instance_id_builder: StringBuilder::new(),
            scope_name_builder: StringBuilder::new(),
            scope_version_builder: StringBuilder::new(),
            resource_attributes_builder: MapBuilder::new(
                Some(map_field_names()),
                StringBuilder::new(),
                new_any_value_struct_builder(),
            ),
            log_attributes_builder: MapBuilder::new(
                Some(map_field_names()),
                StringBuilder::new(),
                new_any_value_struct_builder(),
            ),
            service_name: String::new(),
            first_timestamp: None,
            current_row_count: 0,
        }
    }

    /// Add OTLP log records from protobuf bytes
    pub fn add_from_proto_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        let request = ExportLogsServiceRequest::decode(bytes)
            .context("Failed to decode OTLP ExportLogsServiceRequest")?;
        self.add_from_request(&request)
    }

    /// Add OTLP log records from an ExportLogsServiceRequest
    ///
    /// This method accepts a pre-parsed request, enabling support for
    /// multiple input formats (JSON, JSONL) without duplicating conversion logic.
    pub fn add_from_request(&mut self, request: &ExportLogsServiceRequest) -> Result<()> {
        self.add_from_request_with_flush(request, usize::MAX, &mut |_, _| Ok(()))
    }

    pub fn add_from_request_with_flush<F>(
        &mut self,
        request: &ExportLogsServiceRequest,
        max_rows_per_flush: usize,
        flush_fn: &mut F,
    ) -> Result<()>
    where
        F: FnMut(RecordBatch, LogMetadata) -> Result<()>,
    {
        for resource_logs in &request.resource_logs {
            // Extract resource attributes
            let mut resource_attrs: Vec<(&str, Option<&AnyValue>)> =
                if let Some(resource) = &resource_logs.resource {
                    Vec::with_capacity(resource.attributes.len())
                } else {
                    Vec::new()
                };
            let mut extracted_service_name: Option<&str> = None;
            let mut extracted_service_namespace: Option<&str> = None;
            let mut extracted_service_instance_id: Option<&str> = None;

            if let Some(resource) = &resource_logs.resource {
                for attr in &resource.attributes {
                    let key = attr.key.as_str();

                    if let Some(value) = attr.value.as_ref() {
                        match key {
                            "service.name" => {
                                if let Some(val) = any_value_string(value) {
                                    if self.service_name.is_empty() {
                                        self.service_name = val.to_owned();
                                    }
                                    extracted_service_name = Some(val);
                                }
                                continue;
                            }
                            "service.namespace" => {
                                if let Some(val) = any_value_string(value) {
                                    extracted_service_namespace = Some(val);
                                }
                                continue;
                            }
                            "service.instance.id" => {
                                if let Some(val) = any_value_string(value) {
                                    extracted_service_instance_id = Some(val);
                                }
                                continue;
                            }
                            _ => {}
                        }
                    }

                    if !EXTRACTED_RESOURCE_ATTRS.contains(&key) {
                        resource_attrs.push((key, attr.value.as_ref()));
                    }
                }
            }

            for scope_logs in &resource_logs.scope_logs {
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

                for log_record in &scope_logs.log_records {
                    let timestamp = log_record.time_unix_nano as i64;
                    self.timestamp_builder.append_value(timestamp);
                    self.observed_timestamp_builder
                        .append_value(log_record.observed_time_unix_nano as i64);

                    if self.first_timestamp.is_none() {
                        self.first_timestamp = Some(timestamp);
                    }

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

                    self.severity_text_builder
                        .append_value(&log_record.severity_text);
                    self.severity_number_builder
                        .append_value(log_record.severity_number);

                    append_any_value(&mut self.body_builder, log_record.body.as_ref())?;

                    let service_name_value = extracted_service_name.unwrap_or({
                        if self.service_name.is_empty() {
                            ""
                        } else {
                            self.service_name.as_str()
                        }
                    });
                    self.service_name_builder.append_value(service_name_value);
                    if let Some(ns) = extracted_service_namespace {
                        self.service_namespace_builder.append_value(ns);
                    } else {
                        self.service_namespace_builder.append_null();
                    }
                    if let Some(id) = extracted_service_instance_id {
                        self.service_instance_id_builder.append_value(id);
                    } else {
                        self.service_instance_id_builder.append_null();
                    }

                    self.scope_name_builder.append_value(scope_name);
                    if let Some(ver) = scope_version {
                        self.scope_version_builder.append_value(ver);
                    } else {
                        self.scope_version_builder.append_null();
                    }

                    for &(key, value) in &resource_attrs {
                        self.resource_attributes_builder.keys().append_value(key);
                        append_any_value(self.resource_attributes_builder.values(), value)?;
                    }
                    self.resource_attributes_builder.append(true)?;

                    for attr in &log_record.attributes {
                        self.log_attributes_builder.keys().append_value(&attr.key);
                        append_any_value(
                            self.log_attributes_builder.values(),
                            attr.value.as_ref(),
                        )?;
                    }
                    self.log_attributes_builder.append(true)?;

                    self.current_row_count += 1;

                    if self.current_row_count >= max_rows_per_flush {
                        self.flush(flush_fn)?;
                    }
                }
            }
        }

        Ok(())
    }

    pub fn flush<F>(&mut self, flush_fn: &mut F) -> Result<()>
    where
        F: FnMut(RecordBatch, LogMetadata) -> Result<()>,
    {
        if self.current_row_count == 0 {
            return Ok(());
        }

        let ready = std::mem::take(self);
        let (batch, metadata) = ready.finish()?;
        flush_fn(batch, metadata)?;

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
        let record_count = batch.num_rows();
        let metadata = LogMetadata {
            service_name: if self.service_name.is_empty() {
                "unknown".to_string()
            } else {
                self.service_name
            },
            first_timestamp_nanos: self.first_timestamp.unwrap_or(0),
            record_count,
        };

        Ok((batch, metadata))
    }
}

impl Default for ArrowConverter {
    fn default() -> Self {
        Self::new()
    }
}

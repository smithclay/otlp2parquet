// Convert OTLP log records to Arrow RecordBatch
//
// This is the core processing logic that extracts data from OTLP protobuf
// messages and builds Arrow columns according to the ClickHouse schema.

use anyhow::{Context, Result};
use arrow::array::{
    BinaryBuilder, Int32Builder, RecordBatch, StringBuilder, TimestampMicrosecondBuilder,
    UInt32Builder,
};
use otlp2parquet_proto::opentelemetry::proto::{
    collector::logs::v1::ExportLogsServiceRequest,
    common::v1::AnyValue,
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
};
use prost::Message;
use std::sync::Arc;

use crate::otlp::common::{
    any_value_builder::{any_value_string, any_value_to_json_value},
    builder_helpers::{SPAN_ID_SIZE, TRACE_ID_SIZE},
};
use crate::schema::{otel_logs_schema_arc, EXTRACTED_RESOURCE_ATTRS};

/// JSON-encode attributes map to string for S3 Tables compatibility
fn attributes_to_json_string(attributes: &[(&str, Option<&AnyValue>)]) -> String {
    if attributes.is_empty() {
        return "{}".to_string();
    }

    let mut map = serde_json::Map::new();
    for &(key, value) in attributes {
        let json_value = value
            .map(any_value_to_json_value)
            .unwrap_or(serde_json::Value::Null);
        map.insert(key.to_string(), json_value);
    }

    serde_json::to_string(&map).unwrap_or_else(|_| "{}".to_string())
}

/// JSON-encode body (AnyValue) to string for S3 Tables compatibility
fn body_to_json_string(body: Option<&AnyValue>) -> Option<String> {
    body.map(|v| {
        let json = any_value_to_json_value(v);
        json.to_string()
    })
}

/// Metadata extracted during OTLP parsing
#[derive(Debug, Clone)]
pub struct LogMetadata {
    pub service_name: Arc<str>,
    pub first_timestamp_nanos: i64,
    pub record_count: usize,
}

/// Converts OTLP log records to Arrow RecordBatch
pub struct ArrowConverter {
    // Column builders
    timestamp_builder: TimestampMicrosecondBuilder,
    timestamp_time_builder: TimestampMicrosecondBuilder,
    observed_timestamp_builder: TimestampMicrosecondBuilder,
    trace_id_builder: BinaryBuilder,
    span_id_builder: BinaryBuilder,
    trace_flags_builder: UInt32Builder,
    severity_text_builder: StringBuilder,
    severity_number_builder: Int32Builder,
    body_builder: StringBuilder,
    service_name_builder: StringBuilder,
    service_namespace_builder: StringBuilder,
    service_instance_id_builder: StringBuilder,
    resource_schema_url_builder: StringBuilder,
    scope_name_builder: StringBuilder,
    scope_version_builder: StringBuilder,
    scope_attributes_builder: StringBuilder,
    scope_schema_url_builder: StringBuilder,
    resource_attributes_builder: StringBuilder,
    log_attributes_builder: StringBuilder,

    // Metadata tracking (not part of schema)
    service_name: Arc<str>,
    first_timestamp: Option<i64>,
    current_row_count: usize,
}

/// Default capacity for builders when expected row count is unknown
const DEFAULT_BUILDER_CAPACITY: usize = 1024;

impl ArrowConverter {
    /// Create a new ArrowConverter with default capacity
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_BUILDER_CAPACITY)
    }

    /// Create a new ArrowConverter with specified capacity hint
    pub fn with_capacity(capacity: usize) -> Self {
        let schema = otel_logs_schema_arc();

        Self {
            timestamp_builder: TimestampMicrosecondBuilder::with_capacity(capacity)
                .with_timezone("UTC")
                .with_data_type(schema.field(0).data_type().clone()),
            timestamp_time_builder: TimestampMicrosecondBuilder::with_capacity(capacity)
                .with_timezone("UTC")
                .with_data_type(schema.field(12).data_type().clone()),
            observed_timestamp_builder: TimestampMicrosecondBuilder::with_capacity(capacity)
                .with_timezone("UTC")
                .with_data_type(schema.field(13).data_type().clone()),
            trace_id_builder: BinaryBuilder::with_capacity(
                capacity,
                capacity * TRACE_ID_SIZE as usize,
            ),
            span_id_builder: BinaryBuilder::with_capacity(
                capacity,
                capacity * SPAN_ID_SIZE as usize,
            ),
            trace_flags_builder: UInt32Builder::with_capacity(capacity),
            severity_text_builder: StringBuilder::with_capacity(capacity, capacity * 20),
            severity_number_builder: Int32Builder::with_capacity(capacity),
            body_builder: StringBuilder::with_capacity(capacity, capacity * 256), // JSON-encoded body
            service_name_builder: StringBuilder::with_capacity(capacity, capacity * 32),
            service_namespace_builder: StringBuilder::with_capacity(capacity, capacity * 32),
            service_instance_id_builder: StringBuilder::with_capacity(capacity, capacity * 32),
            resource_schema_url_builder: StringBuilder::with_capacity(capacity, capacity * 64),
            scope_name_builder: StringBuilder::with_capacity(capacity, capacity * 32),
            scope_version_builder: StringBuilder::with_capacity(capacity, capacity * 16),
            scope_attributes_builder: StringBuilder::with_capacity(capacity, capacity * 256), // JSON-encoded attributes
            scope_schema_url_builder: StringBuilder::with_capacity(capacity, capacity * 64),
            resource_attributes_builder: StringBuilder::with_capacity(capacity, capacity * 512), // JSON-encoded attributes
            log_attributes_builder: StringBuilder::with_capacity(capacity, capacity * 1024), // JSON-encoded attributes
            service_name: Arc::from(""),
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

    /// Convert OTLP nanosecond timestamps to microseconds for Iceberg compatibility
    #[inline]
    fn nanos_to_micros(ns: u64) -> i64 {
        ((ns / 1_000).min(i64::MAX as u64)) as i64
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
            self.process_resource_logs(resource_logs, max_rows_per_flush, flush_fn)?;
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
                // Common fields (IDs 1-12)
                Arc::new(self.timestamp_builder.finish()),
                Arc::new(self.trace_id_builder.finish()),
                Arc::new(self.span_id_builder.finish()),
                Arc::new(self.service_name_builder.finish()),
                Arc::new(self.service_namespace_builder.finish()),
                Arc::new(self.service_instance_id_builder.finish()),
                Arc::new(self.resource_attributes_builder.finish()),
                Arc::new(self.resource_schema_url_builder.finish()),
                Arc::new(self.scope_name_builder.finish()),
                Arc::new(self.scope_version_builder.finish()),
                Arc::new(self.scope_attributes_builder.finish()),
                Arc::new(self.scope_schema_url_builder.finish()),
                // Logs-specific fields (IDs 21+)
                Arc::new(self.timestamp_time_builder.finish()),
                Arc::new(self.observed_timestamp_builder.finish()),
                Arc::new(self.trace_flags_builder.finish()),
                Arc::new(self.severity_text_builder.finish()),
                Arc::new(self.severity_number_builder.finish()),
                Arc::new(self.body_builder.finish()),
                Arc::new(self.log_attributes_builder.finish()),
            ],
        )?;

        // Build metadata from tracked values
        let record_count = batch.num_rows();
        let metadata = LogMetadata {
            service_name: Arc::clone(&self.service_name),
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

struct ServiceFields<'a> {
    name: Option<&'a str>,
    namespace: Option<&'a str>,
    instance_id: Option<&'a str>,
}

struct ResourceContext<'a> {
    schema_url: Option<&'a str>,
    attributes: Vec<(&'a str, Option<&'a AnyValue>)>,
    service: ServiceFields<'a>,
}

struct ScopeContext<'a> {
    schema_url: Option<&'a str>,
    name: &'a str,
    version: Option<&'a str>,
    attributes: Vec<(&'a str, Option<&'a AnyValue>)>,
}

impl ArrowConverter {
    fn process_resource_logs<F>(
        &mut self,
        resource_logs: &ResourceLogs,
        max_rows_per_flush: usize,
        flush_fn: &mut F,
    ) -> Result<()>
    where
        F: FnMut(RecordBatch, LogMetadata) -> Result<()>,
    {
        let resource_ctx = self.build_resource_context(resource_logs);

        for scope_logs in &resource_logs.scope_logs {
            let scope_ctx = self.build_scope_context(scope_logs);

            for log_record in &scope_logs.log_records {
                self.append_log_record(
                    log_record,
                    &resource_ctx,
                    &scope_ctx,
                    max_rows_per_flush,
                    flush_fn,
                )?;
            }
        }

        Ok(())
    }

    #[inline]
    fn build_resource_context<'a>(
        &mut self,
        resource_logs: &'a ResourceLogs,
    ) -> ResourceContext<'a> {
        let schema_url =
            (!resource_logs.schema_url.is_empty()).then_some(resource_logs.schema_url.as_str());

        let mut attributes: Vec<(&str, Option<&AnyValue>)> =
            if let Some(resource) = &resource_logs.resource {
                Vec::with_capacity(resource.attributes.len())
            } else {
                Vec::new()
            };

        let mut service_fields = ServiceFields {
            name: None,
            namespace: None,
            instance_id: None,
        };

        if let Some(resource) = &resource_logs.resource {
            for attr in &resource.attributes {
                let key = attr.key.as_str();

                if let Some(value) = attr.value.as_ref() {
                    match key {
                        "service.name" => {
                            if let Some(val) = any_value_string(value) {
                                if self.service_name.is_empty() {
                                    self.service_name = Arc::from(val);
                                }
                                service_fields.name = Some(val);
                            }
                            continue;
                        }
                        "service.namespace" => {
                            if let Some(val) = any_value_string(value) {
                                service_fields.namespace = Some(val);
                            }
                            continue;
                        }
                        "service.instance.id" => {
                            if let Some(val) = any_value_string(value) {
                                service_fields.instance_id = Some(val);
                            }
                            continue;
                        }
                        _ => {}
                    }
                }

                if !EXTRACTED_RESOURCE_ATTRS.contains(&key) {
                    attributes.push((key, attr.value.as_ref()));
                }
            }
        }

        ResourceContext {
            schema_url,
            attributes,
            service: service_fields,
        }
    }

    #[inline]
    fn build_scope_context<'a>(&self, scope_logs: &'a ScopeLogs) -> ScopeContext<'a> {
        let schema_url = if scope_logs.schema_url.is_empty() {
            None
        } else {
            Some(scope_logs.schema_url.as_str())
        };

        let name = scope_logs
            .scope
            .as_ref()
            .map_or("", |scope| scope.name.as_str());
        let version = scope_logs.scope.as_ref().and_then(|scope| {
            if scope.version.is_empty() {
                None
            } else {
                Some(scope.version.as_str())
            }
        });
        let attributes: Vec<(&str, Option<&AnyValue>)> = scope_logs
            .scope
            .as_ref()
            .map(|scope| {
                scope
                    .attributes
                    .iter()
                    .map(|attr| (attr.key.as_str(), attr.value.as_ref()))
                    .collect()
            })
            .unwrap_or_default();

        ScopeContext {
            schema_url,
            name,
            version,
            attributes,
        }
    }

    #[inline]
    fn append_log_record<F>(
        &mut self,
        log_record: &LogRecord,
        resource_ctx: &ResourceContext<'_>,
        scope_ctx: &ScopeContext<'_>,
        max_rows_per_flush: usize,
        flush_fn: &mut F,
    ) -> Result<()>
    where
        F: FnMut(RecordBatch, LogMetadata) -> Result<()>,
    {
        let timestamp = Self::nanos_to_micros(log_record.time_unix_nano);
        self.timestamp_builder.append_value(timestamp);
        self.timestamp_time_builder.append_value(timestamp);
        self.observed_timestamp_builder
            .append_value(Self::nanos_to_micros(log_record.observed_time_unix_nano));

        if self.first_timestamp.is_none() {
            self.first_timestamp = Some(timestamp);
        }

        if log_record.trace_id.len() == TRACE_ID_SIZE as usize {
            self.trace_id_builder.append_value(&log_record.trace_id);
        } else {
            self.trace_id_builder
                .append_value([0u8; TRACE_ID_SIZE as usize]);
        }

        if log_record.span_id.len() == SPAN_ID_SIZE as usize {
            self.span_id_builder.append_value(&log_record.span_id);
        } else {
            self.span_id_builder
                .append_value([0u8; SPAN_ID_SIZE as usize]);
        }

        self.trace_flags_builder.append_value(log_record.flags);
        self.severity_text_builder
            .append_value(&log_record.severity_text);
        self.severity_number_builder
            .append_value(log_record.severity_number);

        // JSON-encode body as string for S3 Tables compatibility
        if let Some(body_json) = body_to_json_string(log_record.body.as_ref()) {
            self.body_builder.append_value(body_json);
        } else {
            self.body_builder.append_null();
        }

        let fallback_name = if self.service_name.is_empty() {
            ""
        } else {
            self.service_name.as_ref()
        };
        let service_name = resource_ctx.service.name.unwrap_or(fallback_name);
        self.service_name_builder.append_value(service_name);

        if let Some(ns) = resource_ctx.service.namespace {
            self.service_namespace_builder.append_value(ns);
        } else {
            self.service_namespace_builder.append_null();
        }

        if let Some(id) = resource_ctx.service.instance_id {
            self.service_instance_id_builder.append_value(id);
        } else {
            self.service_instance_id_builder.append_null();
        }

        if let Some(url) = resource_ctx.schema_url {
            self.resource_schema_url_builder.append_value(url);
        } else {
            self.resource_schema_url_builder.append_null();
        }

        self.scope_name_builder.append_value(scope_ctx.name);
        if let Some(version) = scope_ctx.version {
            self.scope_version_builder.append_value(version);
        } else {
            self.scope_version_builder.append_null();
        }

        // JSON-encode scope attributes as string for S3 Tables compatibility
        let scope_attrs_json = attributes_to_json_string(&scope_ctx.attributes);
        self.scope_attributes_builder.append_value(scope_attrs_json);

        if let Some(url) = scope_ctx.schema_url {
            self.scope_schema_url_builder.append_value(url);
        } else {
            self.scope_schema_url_builder.append_null();
        }

        // JSON-encode resource attributes as string for S3 Tables compatibility
        let resource_attrs_json = attributes_to_json_string(&resource_ctx.attributes);
        self.resource_attributes_builder
            .append_value(resource_attrs_json);

        // JSON-encode log attributes as string for S3 Tables compatibility
        let log_attrs: Vec<(&str, Option<&AnyValue>)> = log_record
            .attributes
            .iter()
            .map(|attr| (attr.key.as_str(), attr.value.as_ref()))
            .collect();
        let log_attrs_json = attributes_to_json_string(&log_attrs);
        self.log_attributes_builder.append_value(log_attrs_json);

        self.current_row_count += 1;

        if self.current_row_count >= max_rows_per_flush {
            self.flush(flush_fn)?;
        }

        Ok(())
    }
}

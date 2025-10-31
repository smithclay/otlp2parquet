// Convert OTLP log records to Arrow RecordBatch
//
// This is the core processing logic that extracts data from OTLP protobuf
// messages and builds Arrow columns according to the ClickHouse schema.

use anyhow::{Context, Result};
use arrow::array::{
    builder::MapFieldNames, ArrayBuilder, BinaryBuilder, BooleanBuilder, FixedSizeBinaryBuilder,
    Float64Builder, Int32Builder, Int64Builder, LargeStringBuilder, MapBuilder, RecordBatch,
    StringBuilder, StructBuilder, TimestampNanosecondBuilder, UInt32Builder,
};
use otlp2parquet_proto::opentelemetry::proto::{
    collector::logs::v1::ExportLogsServiceRequest,
    common::v1::{any_value, AnyValue},
};
use prost::Message;
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};
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

fn new_any_value_struct_builder() -> StructBuilder {
    let fields = crate::schema::any_value_fields();
    StructBuilder::new(
        fields.clone(),
        vec![
            Box::new(StringBuilder::new()) as Box<dyn ArrayBuilder>,
            Box::new(StringBuilder::new()),
            Box::new(BooleanBuilder::new()),
            Box::new(Int64Builder::new()),
            Box::new(Float64Builder::new()),
            Box::new(BinaryBuilder::new()),
            Box::new(LargeStringBuilder::new()),
        ],
    )
}

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
            let mut resource_attrs: Vec<(String, Option<AnyValue>)> =
                if let Some(resource) = &resource_logs.resource {
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

                    if let Some(value) = attr.value.as_ref() {
                        match key.as_str() {
                            "service.name" => {
                                if let Some(val) = any_value_string(value) {
                                    extracted_service_name = val.clone();
                                    if self.service_name.is_empty() {
                                        self.service_name = val;
                                    }
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

                    if !EXTRACTED_RESOURCE_ATTRS.contains(&key.as_str()) {
                        resource_attrs.push((key.clone(), attr.value.clone()));
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

                    self.scope_name_builder.append_value(scope_name);
                    if let Some(ver) = scope_version {
                        self.scope_version_builder.append_value(ver);
                    } else {
                        self.scope_version_builder.append_null();
                    }

                    for (key, value) in &resource_attrs {
                        self.resource_attributes_builder.keys().append_value(key);
                        append_any_value(
                            self.resource_attributes_builder.values(),
                            value.as_ref(),
                        )?;
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

fn append_any_value(builder: &mut StructBuilder, any_val: Option<&AnyValue>) -> Result<()> {
    const TYPE_INDEX: usize = 0;
    const STRING_INDEX: usize = 1;
    const BOOL_INDEX: usize = 2;
    const INT_INDEX: usize = 3;
    const DOUBLE_INDEX: usize = 4;
    const BYTES_INDEX: usize = 5;
    const JSON_INDEX: usize = 6;

    if let Some(any_val) = any_val {
        if let Some(inner) = any_val.value.as_ref() {
            let mut string_value: Option<&str> = None;
            let mut bool_value: Option<bool> = None;
            let mut int_value: Option<i64> = None;
            let mut double_value: Option<f64> = None;
            let mut bytes_value: Option<&[u8]> = None;
            let mut json_value: Option<String> = None;
            let type_name;

            match inner {
                any_value::Value::StringValue(s) => {
                    type_name = "string";
                    string_value = Some(s);
                }
                any_value::Value::BoolValue(b) => {
                    type_name = "bool";
                    bool_value = Some(*b);
                }
                any_value::Value::IntValue(i) => {
                    type_name = "int";
                    int_value = Some(*i);
                }
                any_value::Value::DoubleValue(d) => {
                    type_name = "double";
                    double_value = Some(*d);
                }
                any_value::Value::BytesValue(b) => {
                    type_name = "bytes";
                    bytes_value = Some(b);
                }
                any_value::Value::ArrayValue(arr) => {
                    type_name = "array";
                    let json =
                        JsonValue::Array(arr.values.iter().map(any_value_to_json_value).collect());
                    json_value = Some(serde_json::to_string(&json)?);
                }
                any_value::Value::KvlistValue(kv) => {
                    type_name = "kvlist";
                    let mut map = JsonMap::new();
                    for kv in &kv.values {
                        let value_json = kv
                            .value
                            .as_ref()
                            .map(any_value_to_json_value)
                            .unwrap_or(JsonValue::Null);
                        map.insert(kv.key.clone(), value_json);
                    }
                    json_value = Some(serde_json::to_string(&JsonValue::Object(map))?);
                }
            }

            {
                let type_builder = builder
                    .field_builder::<StringBuilder>(TYPE_INDEX)
                    .expect("type builder");
                type_builder.append_value(type_name);
            }

            {
                let string_builder = builder
                    .field_builder::<StringBuilder>(STRING_INDEX)
                    .expect("string builder");
                if let Some(val) = string_value {
                    string_builder.append_value(val);
                } else {
                    string_builder.append_null();
                }
            }

            {
                let bool_builder = builder
                    .field_builder::<BooleanBuilder>(BOOL_INDEX)
                    .expect("bool builder");
                if let Some(val) = bool_value {
                    bool_builder.append_value(val);
                } else {
                    bool_builder.append_null();
                }
            }

            {
                let int_builder = builder
                    .field_builder::<Int64Builder>(INT_INDEX)
                    .expect("int builder");
                if let Some(val) = int_value {
                    int_builder.append_value(val);
                } else {
                    int_builder.append_null();
                }
            }

            {
                let double_builder = builder
                    .field_builder::<Float64Builder>(DOUBLE_INDEX)
                    .expect("double builder");
                if let Some(val) = double_value {
                    double_builder.append_value(val);
                } else {
                    double_builder.append_null();
                }
            }

            {
                let bytes_builder = builder
                    .field_builder::<BinaryBuilder>(BYTES_INDEX)
                    .expect("bytes builder");
                if let Some(val) = bytes_value {
                    bytes_builder.append_value(val);
                } else {
                    bytes_builder.append_null();
                }
            }

            {
                let json_builder = builder
                    .field_builder::<LargeStringBuilder>(JSON_INDEX)
                    .expect("json builder");
                if let Some(val) = json_value {
                    json_builder.append_value(&val);
                } else {
                    json_builder.append_null();
                }
            }

            builder.append(true);
            return Ok(());
        }
    }

    builder
        .field_builder::<StringBuilder>(TYPE_INDEX)
        .expect("type builder")
        .append_null();
    builder
        .field_builder::<StringBuilder>(STRING_INDEX)
        .expect("string builder")
        .append_null();
    builder
        .field_builder::<BooleanBuilder>(BOOL_INDEX)
        .expect("bool builder")
        .append_null();
    builder
        .field_builder::<Int64Builder>(INT_INDEX)
        .expect("int builder")
        .append_null();
    builder
        .field_builder::<Float64Builder>(DOUBLE_INDEX)
        .expect("double builder")
        .append_null();
    builder
        .field_builder::<BinaryBuilder>(BYTES_INDEX)
        .expect("bytes builder")
        .append_null();
    builder
        .field_builder::<LargeStringBuilder>(JSON_INDEX)
        .expect("json builder")
        .append_null();
    builder.append(false);
    Ok(())
}

fn any_value_string(any_val: &AnyValue) -> Option<String> {
    match any_val.value.as_ref()? {
        any_value::Value::StringValue(s) => Some(s.clone()),
        _ => None,
    }
}

fn any_value_to_json_value(any_val: &AnyValue) -> JsonValue {
    match any_val.value.as_ref() {
        Some(any_value::Value::StringValue(s)) => JsonValue::String(s.clone()),
        Some(any_value::Value::BoolValue(b)) => JsonValue::Bool(*b),
        Some(any_value::Value::IntValue(i)) => JsonValue::Number(JsonNumber::from(*i)),
        Some(any_value::Value::DoubleValue(d)) => JsonNumber::from_f64(*d)
            .map(JsonValue::Number)
            .unwrap_or_else(|| JsonValue::String(d.to_string())),
        Some(any_value::Value::BytesValue(b)) => JsonValue::String(format!("bytes:{}", b.len())),
        Some(any_value::Value::ArrayValue(arr)) => {
            JsonValue::Array(arr.values.iter().map(any_value_to_json_value).collect())
        }
        Some(any_value::Value::KvlistValue(kv)) => {
            let mut map = JsonMap::new();
            for entry in &kv.values {
                let value = entry
                    .value
                    .as_ref()
                    .map(any_value_to_json_value)
                    .unwrap_or(JsonValue::Null);
                map.insert(entry.key.clone(), value);
            }
            JsonValue::Object(map)
        }
        None => JsonValue::Null,
    }
}

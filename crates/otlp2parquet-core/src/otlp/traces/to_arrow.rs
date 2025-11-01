use std::convert::{TryFrom, TryInto};
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{
    Int64Builder, ListBuilder, MapBuilder, RecordBatch, StringBuilder, TimestampNanosecondBuilder,
};
use arrow::datatypes::DataType;
use otlp2parquet_proto::opentelemetry::proto::{
    collector::trace::v1::ExportTraceServiceRequest,
    common::v1::{any_value, AnyValue, InstrumentationScope, KeyValue},
    resource::v1::Resource,
    trace::v1::{span, status, ResourceSpans, ScopeSpans, Span},
};

use crate::otlp::common::{
    any_value_builder::{any_value_string, any_value_to_json_value},
    builder_helpers::map_field_names,
};
use crate::otlp::field_names::semconv;
use crate::schema::{otel_traces_schema_arc, EXTRACTED_RESOURCE_ATTRS};

use super::format::TraceRequest;

/// Metadata extracted from OTLP trace export requests during conversion.
#[derive(Debug, Clone)]
pub struct TraceMetadata {
    pub service_name: Arc<str>,
    pub first_timestamp_nanos: i64,
    pub span_count: usize,
}

/// Converts OTLP trace data to Arrow record batches.
pub struct TraceArrowConverter;

impl TraceArrowConverter {
    /// Convert the supplied trace request into an Arrow record batch and metadata.
    pub fn convert(request: &TraceRequest) -> Result<(Vec<RecordBatch>, TraceMetadata)> {
        let span_count = estimate_span_count(request);
        let mut builder = TraceArrowBuilder::with_capacity(span_count.max(1));
        builder.add_request(request)?;
        let (batch, metadata) = builder.finish()?;
        Ok((vec![batch], metadata))
    }
}

fn estimate_span_count(request: &ExportTraceServiceRequest) -> usize {
    request
        .resource_spans
        .iter()
        .map(|resource_spans| {
            resource_spans
                .scope_spans
                .iter()
                .map(|scope_spans| scope_spans.spans.len())
                .sum::<usize>()
        })
        .sum()
}

struct TraceArrowBuilder {
    timestamp_builder: TimestampNanosecondBuilder,
    trace_id_builder: StringBuilder,
    span_id_builder: StringBuilder,
    parent_span_id_builder: StringBuilder,
    trace_state_builder: StringBuilder,
    span_name_builder: StringBuilder,
    span_kind_builder: StringBuilder,
    service_name_builder: StringBuilder,
    resource_attributes_builder: MapBuilder<StringBuilder, StringBuilder>,
    scope_name_builder: StringBuilder,
    scope_version_builder: StringBuilder,
    span_attributes_builder: MapBuilder<StringBuilder, StringBuilder>,
    duration_builder: Int64Builder,
    status_code_builder: StringBuilder,
    status_message_builder: StringBuilder,
    events_timestamp_builder: ListBuilder<TimestampNanosecondBuilder>,
    events_name_builder: ListBuilder<StringBuilder>,
    events_attributes_builder: ListBuilder<MapBuilder<StringBuilder, StringBuilder>>,
    links_trace_id_builder: ListBuilder<StringBuilder>,
    links_span_id_builder: ListBuilder<StringBuilder>,
    links_trace_state_builder: ListBuilder<StringBuilder>,
    links_attributes_builder: ListBuilder<MapBuilder<StringBuilder, StringBuilder>>,
    service_name: Arc<str>,
    first_timestamp: Option<i64>,
    span_count: usize,
}

struct ResourceContext<'a> {
    service_name: Option<&'a str>,
    attributes: Vec<&'a KeyValue>,
}

struct ScopeContext<'a> {
    name: Option<&'a str>,
    version: Option<&'a str>,
}

impl TraceArrowBuilder {
    fn with_capacity(capacity: usize) -> Self {
        let schema = otel_traces_schema_arc();

        let timestamp_builder = TimestampNanosecondBuilder::with_capacity(capacity)
            .with_timezone("UTC")
            .with_data_type(schema.field(0).data_type().clone());

        let events_timestamp_field = match schema.field(15).data_type() {
            DataType::List(field) => field.clone(),
            _ => unreachable!("unexpected data type for events timestamp list"),
        };
        let events_name_field = match schema.field(16).data_type() {
            DataType::List(field) => field.clone(),
            _ => unreachable!("unexpected data type for events name list"),
        };
        let events_attributes_field = match schema.field(17).data_type() {
            DataType::List(field) => field.clone(),
            _ => unreachable!("unexpected data type for events attributes list"),
        };
        let links_trace_id_field = match schema.field(18).data_type() {
            DataType::List(field) => field.clone(),
            _ => unreachable!("unexpected data type for links trace id list"),
        };
        let links_span_id_field = match schema.field(19).data_type() {
            DataType::List(field) => field.clone(),
            _ => unreachable!("unexpected data type for links span id list"),
        };
        let links_trace_state_field = match schema.field(20).data_type() {
            DataType::List(field) => field.clone(),
            _ => unreachable!("unexpected data type for links trace state list"),
        };
        let links_attributes_field = match schema.field(21).data_type() {
            DataType::List(field) => field.clone(),
            _ => unreachable!("unexpected data type for links attributes list"),
        };

        let events_timestamp_values = TimestampNanosecondBuilder::with_capacity(capacity * 4)
            .with_timezone("UTC")
            .with_data_type(events_timestamp_field.data_type().clone());

        let events_timestamp_builder =
            ListBuilder::with_capacity(events_timestamp_values, capacity)
                .with_field(events_timestamp_field.clone());

        let events_attributes_values = MapBuilder::new(
            Some(map_field_names()),
            StringBuilder::new(),
            StringBuilder::new(),
        );

        let events_attributes_builder =
            ListBuilder::with_capacity(events_attributes_values, capacity)
                .with_field(events_attributes_field.clone());

        let events_name_values = StringBuilder::with_capacity(capacity * 4, capacity * 32);
        let events_name_builder = ListBuilder::with_capacity(events_name_values, capacity)
            .with_field(events_name_field.clone());

        let links_trace_id_values = StringBuilder::with_capacity(capacity * 2, capacity * 32);
        let links_trace_id_builder = ListBuilder::with_capacity(links_trace_id_values, capacity)
            .with_field(links_trace_id_field.clone());

        let links_span_id_values = StringBuilder::with_capacity(capacity * 2, capacity * 16);
        let links_span_id_builder = ListBuilder::with_capacity(links_span_id_values, capacity)
            .with_field(links_span_id_field.clone());

        let links_trace_state_values = StringBuilder::with_capacity(capacity * 2, capacity * 32);
        let links_trace_state_builder =
            ListBuilder::with_capacity(links_trace_state_values, capacity)
                .with_field(links_trace_state_field.clone());

        let links_attributes_values = MapBuilder::new(
            Some(map_field_names()),
            StringBuilder::new(),
            StringBuilder::new(),
        );

        let links_attributes_builder =
            ListBuilder::with_capacity(links_attributes_values, capacity)
                .with_field(links_attributes_field.clone());

        Self {
            timestamp_builder,
            trace_id_builder: StringBuilder::with_capacity(capacity, capacity * 32),
            span_id_builder: StringBuilder::with_capacity(capacity, capacity * 16),
            parent_span_id_builder: StringBuilder::with_capacity(capacity, capacity * 16),
            trace_state_builder: StringBuilder::with_capacity(capacity, capacity * 32),
            span_name_builder: StringBuilder::with_capacity(capacity, capacity * 32),
            span_kind_builder: StringBuilder::with_capacity(capacity, capacity * 24),
            service_name_builder: StringBuilder::with_capacity(capacity, capacity * 32),
            resource_attributes_builder: MapBuilder::new(
                Some(map_field_names()),
                StringBuilder::with_capacity(capacity * 4, capacity * 64),
                StringBuilder::with_capacity(capacity * 4, capacity * 128),
            ),
            scope_name_builder: StringBuilder::with_capacity(capacity, capacity * 24),
            scope_version_builder: StringBuilder::with_capacity(capacity, capacity * 24),
            span_attributes_builder: MapBuilder::new(
                Some(map_field_names()),
                StringBuilder::with_capacity(capacity * 8, capacity * 64),
                StringBuilder::with_capacity(capacity * 8, capacity * 128),
            ),
            duration_builder: Int64Builder::with_capacity(capacity),
            status_code_builder: StringBuilder::with_capacity(capacity, capacity * 24),
            status_message_builder: StringBuilder::with_capacity(capacity, capacity * 64),
            events_timestamp_builder,
            events_name_builder,
            events_attributes_builder,
            links_trace_id_builder,
            links_span_id_builder,
            links_trace_state_builder,
            links_attributes_builder,
            service_name: Arc::from(""),
            first_timestamp: None,
            span_count: 0,
        }
    }

    fn add_request(&mut self, request: &ExportTraceServiceRequest) -> Result<()> {
        for resource_spans in &request.resource_spans {
            self.process_resource_spans(resource_spans)?;
        }
        Ok(())
    }

    fn process_resource_spans(&mut self, resource_spans: &ResourceSpans) -> Result<()> {
        let resource_ctx = self.build_resource_context(resource_spans);

        for scope_spans in &resource_spans.scope_spans {
            let scope_ctx = self.build_scope_context(scope_spans);

            for span in &scope_spans.spans {
                self.append_span(span, &resource_ctx, &scope_ctx)?;
            }
        }

        Ok(())
    }

    fn build_resource_context<'a>(
        &mut self,
        resource_spans: &'a ResourceSpans,
    ) -> ResourceContext<'a> {
        let mut service_name = None;
        let mut attributes: Vec<&'a KeyValue> = Vec::new();

        if let Some(Resource {
            attributes: resource_attrs,
            ..
        }) = &resource_spans.resource
        {
            attributes.reserve(resource_attrs.len());

            for attr in resource_attrs {
                if attr.key == semconv::SERVICE_NAME {
                    if let Some(value) = attr.value.as_ref().and_then(any_value_string) {
                        if !value.is_empty() {
                            service_name = Some(value);
                            if self.service_name.is_empty() {
                                self.service_name = Arc::from(value);
                            }
                        }
                    }
                }

                if !EXTRACTED_RESOURCE_ATTRS.contains(&attr.key.as_str()) {
                    attributes.push(attr);
                }
            }
        }

        ResourceContext {
            service_name,
            attributes,
        }
    }

    fn build_scope_context<'a>(&self, scope_spans: &'a ScopeSpans) -> ScopeContext<'a> {
        let (name, version) = scope_spans
            .scope
            .as_ref()
            .map(|InstrumentationScope { name, version, .. }| {
                let name_ref = if name.is_empty() {
                    None
                } else {
                    Some(name.as_str())
                };
                let version_ref = if version.is_empty() {
                    None
                } else {
                    Some(version.as_str())
                };
                (name_ref, version_ref)
            })
            .unwrap_or((None, None));

        ScopeContext { name, version }
    }

    fn append_span(
        &mut self,
        span: &Span,
        resource_ctx: &ResourceContext<'_>,
        scope_ctx: &ScopeContext<'_>,
    ) -> Result<()> {
        let timestamp = Self::clamp_nanos(span.start_time_unix_nano);
        self.timestamp_builder.append_value(timestamp);
        self.update_first_timestamp(timestamp);

        append_hex_value(&mut self.trace_id_builder, &span.trace_id);
        append_hex_value(&mut self.span_id_builder, &span.span_id);

        if span.parent_span_id.is_empty() {
            self.parent_span_id_builder.append_null();
        } else {
            append_hex_value(&mut self.parent_span_id_builder, &span.parent_span_id);
        }

        if span.trace_state.is_empty() {
            self.trace_state_builder.append_null();
        } else {
            self.trace_state_builder
                .append_value(span.trace_state.as_str());
        }

        self.span_name_builder.append_value(span.name.as_str());

        let kind = span::SpanKind::try_from(span.kind).unwrap_or(span::SpanKind::Unspecified);
        self.span_kind_builder.append_value(kind.as_str_name());

        if let Some(service_name) = resource_ctx.service_name {
            self.service_name_builder.append_value(service_name);
        } else {
            self.service_name_builder.append_null();
        }

        self.append_resource_attributes(resource_ctx)?;
        self.append_scope(scope_ctx);
        self.append_span_attributes(span)?;

        let duration = Self::compute_duration(span.start_time_unix_nano, span.end_time_unix_nano);
        self.duration_builder.append_value(duration);

        if let Some(status) = span.status.as_ref() {
            let code = status::StatusCode::try_from(status.code)
                .unwrap_or(status::StatusCode::Unset)
                .as_str_name();
            self.status_code_builder.append_value(code);

            if status.message.is_empty() {
                self.status_message_builder.append_null();
            } else {
                self.status_message_builder
                    .append_value(status.message.as_str());
            }
        } else {
            self.status_code_builder.append_null();
            self.status_message_builder.append_null();
        }

        self.append_events(&span.events)?;
        self.append_links(&span.links)?;

        self.span_count += 1;

        Ok(())
    }

    fn append_resource_attributes(&mut self, resource_ctx: &ResourceContext<'_>) -> Result<()> {
        for attr in &resource_ctx.attributes {
            self.resource_attributes_builder
                .keys()
                .append_value(attr.key.as_str());
            append_attribute_value(
                self.resource_attributes_builder.values(),
                attr.value.as_ref(),
            )?;
        }

        self.resource_attributes_builder.append(true)?;
        Ok(())
    }

    fn append_scope(&mut self, scope_ctx: &ScopeContext<'_>) {
        if let Some(name) = scope_ctx.name {
            self.scope_name_builder.append_value(name);
        } else {
            self.scope_name_builder.append_null();
        }

        if let Some(version) = scope_ctx.version {
            self.scope_version_builder.append_value(version);
        } else {
            self.scope_version_builder.append_null();
        }
    }

    fn append_span_attributes(&mut self, span: &Span) -> Result<()> {
        for attr in &span.attributes {
            self.span_attributes_builder
                .keys()
                .append_value(attr.key.as_str());
            append_attribute_value(self.span_attributes_builder.values(), attr.value.as_ref())?;
        }

        self.span_attributes_builder.append(true)?;
        Ok(())
    }

    fn append_events(&mut self, events: &[span::Event]) -> Result<()> {
        {
            let timestamps = self.events_timestamp_builder.values();
            let names = self.events_name_builder.values();
            let attributes = self.events_attributes_builder.values();

            for event in events {
                timestamps.append_value(Self::clamp_nanos(event.time_unix_nano));
                names.append_value(event.name.as_str());

                for attr in &event.attributes {
                    attributes.keys().append_value(attr.key.as_str());
                    append_attribute_value(attributes.values(), attr.value.as_ref())?;
                }

                attributes.append(true)?;
            }
        }

        self.events_timestamp_builder.append(true);
        self.events_name_builder.append(true);
        self.events_attributes_builder.append(true);
        Ok(())
    }

    fn append_links(&mut self, links: &[span::Link]) -> Result<()> {
        {
            let trace_ids = self.links_trace_id_builder.values();
            let span_ids = self.links_span_id_builder.values();
            let trace_states = self.links_trace_state_builder.values();
            let attributes = self.links_attributes_builder.values();

            for link in links {
                append_hex_value(trace_ids, &link.trace_id);
                append_hex_value(span_ids, &link.span_id);

                if link.trace_state.is_empty() {
                    trace_states.append_null();
                } else {
                    trace_states.append_value(link.trace_state.as_str());
                }

                for attr in &link.attributes {
                    attributes.keys().append_value(attr.key.as_str());
                    append_attribute_value(attributes.values(), attr.value.as_ref())?;
                }

                attributes.append(true)?;
            }
        }

        self.links_trace_id_builder.append(true);
        self.links_span_id_builder.append(true);
        self.links_trace_state_builder.append(true);
        self.links_attributes_builder.append(true);
        Ok(())
    }

    fn finish(mut self) -> Result<(RecordBatch, TraceMetadata)> {
        let schema = otel_traces_schema_arc();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(self.timestamp_builder.finish()),
                Arc::new(self.trace_id_builder.finish()),
                Arc::new(self.span_id_builder.finish()),
                Arc::new(self.parent_span_id_builder.finish()),
                Arc::new(self.trace_state_builder.finish()),
                Arc::new(self.span_name_builder.finish()),
                Arc::new(self.span_kind_builder.finish()),
                Arc::new(self.service_name_builder.finish()),
                Arc::new(self.resource_attributes_builder.finish()),
                Arc::new(self.scope_name_builder.finish()),
                Arc::new(self.scope_version_builder.finish()),
                Arc::new(self.span_attributes_builder.finish()),
                Arc::new(self.duration_builder.finish()),
                Arc::new(self.status_code_builder.finish()),
                Arc::new(self.status_message_builder.finish()),
                Arc::new(self.events_timestamp_builder.finish()),
                Arc::new(self.events_name_builder.finish()),
                Arc::new(self.events_attributes_builder.finish()),
                Arc::new(self.links_trace_id_builder.finish()),
                Arc::new(self.links_span_id_builder.finish()),
                Arc::new(self.links_trace_state_builder.finish()),
                Arc::new(self.links_attributes_builder.finish()),
            ],
        )?;

        let metadata = TraceMetadata {
            service_name: Arc::clone(&self.service_name),
            first_timestamp_nanos: self.first_timestamp.unwrap_or(0),
            span_count: self.span_count,
        };

        Ok((batch, metadata))
    }

    fn update_first_timestamp(&mut self, timestamp: i64) {
        match self.first_timestamp {
            Some(current) if timestamp >= current => {}
            _ => self.first_timestamp = Some(timestamp),
        }
    }

    fn clamp_nanos(ns: u64) -> i64 {
        (ns.min(i64::MAX as u64)) as i64
    }

    fn compute_duration(start: u64, end: u64) -> i64 {
        if end >= start {
            (end - start)
                .min(i64::MAX as u64)
                .try_into()
                .unwrap_or(i64::MAX)
        } else {
            0
        }
    }
}

fn append_attribute_value(builder: &mut StringBuilder, value: Option<&AnyValue>) -> Result<()> {
    if let Some(any) = value {
        match any.value.as_ref() {
            Some(any_value::Value::StringValue(s)) => builder.append_value(s),
            _ => {
                let json = serde_json::to_string(&any_value_to_json_value(any))
                    .context("Failed to encode OTLP AnyValue as JSON string")?;
                builder.append_value(&json);
            }
        }
    } else {
        builder.append_null();
    }

    Ok(())
}

fn append_hex_value(builder: &mut StringBuilder, bytes: &[u8]) {
    let encoded = hex::encode(bytes);
    builder.append_value(&encoded);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::otlp::{common::format::InputFormat, traces::parse_otlp_trace_request};
    use arrow::array::{Array, Int64Array, ListArray, StringArray};
    use otlp2parquet_proto::opentelemetry::proto::trace::v1::Status;

    #[test]
    fn converts_empty_request() {
        let request = TraceRequest {
            resource_spans: Vec::new(),
        };

        let (batches, metadata) = TraceArrowConverter::convert(&request).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 0);
        assert_eq!(metadata.span_count, 0);
        assert_eq!(metadata.first_timestamp_nanos, 0);
        assert!(metadata.service_name.is_empty());
    }

    #[test]
    fn converts_basic_span() {
        let span = Span {
            trace_id: vec![0x11; 16],
            span_id: vec![0x22; 8],
            parent_span_id: Vec::new(),
            trace_state: String::new(),
            name: "demo-span".into(),
            kind: span::SpanKind::Server as i32,
            start_time_unix_nano: 1_000,
            end_time_unix_nano: 2_500,
            attributes: vec![KeyValue {
                key: "span.attr".into(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue("value".into())),
                }),
            }],
            events: vec![span::Event {
                time_unix_nano: 1_500,
                name: "evt".into(),
                attributes: vec![],
                dropped_attributes_count: 0,
            }],
            links: vec![span::Link {
                trace_id: vec![0x33; 16],
                span_id: vec![0x44; 8],
                trace_state: String::new(),
                attributes: Vec::new(),
                dropped_attributes_count: 0,
                flags: 0,
            }],
            status: Some(Status {
                message: "ok".into(),
                code: status::StatusCode::Ok as i32,
            }),
            ..Default::default()
        };

        let resource = Resource {
            attributes: vec![KeyValue {
                key: semconv::SERVICE_NAME.into(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue("svc".into())),
                }),
            }],
            ..Default::default()
        };

        let scope_spans = ScopeSpans {
            scope: Some(InstrumentationScope {
                name: "instrumentation".into(),
                version: "1.0".into(),
                ..Default::default()
            }),
            spans: vec![span],
            ..Default::default()
        };

        let request = TraceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(resource),
                scope_spans: vec![scope_spans],
                ..Default::default()
            }],
        };

        let (batches, metadata) = TraceArrowConverter::convert(&request).unwrap();
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 1);

        assert_eq!(metadata.span_count, 1);
        assert_eq!(metadata.first_timestamp_nanos, 1_000);
        assert_eq!(metadata.service_name.as_ref(), "svc");

        let trace_ids = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(trace_ids.value(0), hex::encode(vec![0x11; 16]));

        let durations = batch
            .column(12)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(durations.value(0), 1_500);
    }

    #[test]
    fn converts_trace_json_fixture() {
        let json_bytes = include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../testdata/trace.json"
        ));

        let request = parse_otlp_trace_request(json_bytes, InputFormat::Json).unwrap();
        let (batches, metadata) = TraceArrowConverter::convert(&request).unwrap();

        assert_eq!(metadata.service_name.as_ref(), "frontend-proxy");
        assert_eq!(metadata.first_timestamp_nanos, 1_760_738_064_624_180_000);
        assert_eq!(metadata.span_count, 2);

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 2);

        let span_names = batch
            .column(5)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(span_names.value(0), "router frontend egress");
        assert_eq!(span_names.value(1), "ingress");

        let durations = batch
            .column(12)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(durations.value(0), 4_021_000);
        assert_eq!(durations.value(1), 4_328_000);

        let events = batch
            .column(16)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(events.value_length(0), 0);
        assert_eq!(events.value_length(1), 0);
    }

    #[test]
    fn converts_trace_protobuf_fixture() {
        let proto_bytes = include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../testdata/trace.pb"
        ));

        let request =
            parse_otlp_trace_request(proto_bytes, InputFormat::Protobuf).expect("protobuf parse");
        let (batches, metadata) = TraceArrowConverter::convert(&request).unwrap();

        assert_eq!(metadata.service_name.as_ref(), "frontend-proxy");
        assert_eq!(metadata.first_timestamp_nanos, 1_760_738_064_624_180_000);
        assert_eq!(metadata.span_count, 2);

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
    }

    #[test]
    fn converts_jsonl_trace_fixture() {
        let jsonl_bytes = include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../testdata/traces.jsonl"
        ));

        let request = parse_otlp_trace_request(jsonl_bytes, InputFormat::Jsonl).unwrap();
        let (batches, metadata) = TraceArrowConverter::convert(&request).unwrap();

        assert_eq!(metadata.span_count, 63);
        assert_eq!(metadata.first_timestamp_nanos, 1_760_738_032_995_001_500);
        assert_eq!(metadata.service_name.as_ref(), "frontend-proxy");

        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 63);

        let events = batch
            .column(16)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let event_names = events
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let mut total_events = 0usize;
        let mut saw_product_found = false;
        for row in 0..batch.num_rows() {
            let len = events.value_length(row) as usize;
            total_events += len;

            if len > 0 {
                let values = events.value(row);
                let names = values.as_any().downcast_ref::<StringArray>().unwrap();
                for idx in 0..names.len() {
                    if names.value(idx) == "Product Found" {
                        saw_product_found = true;
                    }
                }
            }
        }

        assert!(total_events > 0);
        assert!(saw_product_found);

        let links = batch
            .column(18)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let mut spans_with_links = 0;
        for row in 0..batch.num_rows() {
            if links.value_length(row) > 0 {
                spans_with_links += 1;
            }
        }

        assert!(spans_with_links >= 2);
        assert!(event_names.len() >= total_events);
    }

    #[test]
    fn converts_protobuf_traces_fixture() {
        let proto_bytes = include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../testdata/traces.pb"
        ));

        let request =
            parse_otlp_trace_request(proto_bytes, InputFormat::Protobuf).expect("protobuf parse");
        let (batches, metadata) = TraceArrowConverter::convert(&request).unwrap();

        assert_eq!(metadata.span_count, 63);
        assert_eq!(metadata.first_timestamp_nanos, 1_760_738_032_995_001_500);
        assert_eq!(metadata.service_name.as_ref(), "frontend-proxy");

        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 63);
    }
}

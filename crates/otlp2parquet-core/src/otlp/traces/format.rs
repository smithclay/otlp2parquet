use anyhow::Result;
use otlp2parquet_proto::opentelemetry::proto::collector::trace::v1::ExportTraceServiceRequest;

use crate::otlp::common::{
    json_normalizer::normalise_json_value, parse_request, InputFormat, OtlpSignalRequest,
};

/// Type alias representing an OTLP trace export request.
pub type TraceRequest = ExportTraceServiceRequest;

/// Parse OTLP traces from bytes in the specified format.
pub fn parse_otlp_trace_request(bytes: &[u8], format: InputFormat) -> Result<TraceRequest> {
    parse_request(bytes, format, Some(normalise_json_value))
}

impl OtlpSignalRequest for ExportTraceServiceRequest {
    const JSONL_EMPTY_ERROR: &'static str = "JSONL input contained no valid trace records";

    fn merge(&mut self, mut other: Self) {
        self.resource_spans.append(&mut other.resource_spans);
    }

    fn is_empty(&self) -> bool {
        self.resource_spans.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::otlp::common::{
        any_value_builder::any_value_string, json_normalizer::normalise_json_value,
    };
    use crate::otlp::field_names::{otlp, semconv};
    use otlp2parquet_proto::opentelemetry::proto::{
        common::v1::{AnyValue, KeyValue},
        trace::v1::{span, ResourceSpans, ScopeSpans, Span},
    };
    use prost::Message;
    use serde_json::Value as JsonValue;

    #[test]
    fn parses_protobuf_payload() {
        let span = Span {
            trace_id: vec![0xAA; 16],
            span_id: vec![0xBB; 8],
            parent_span_id: Vec::new(),
            kind: span::SpanKind::Server as i32,
            ..Default::default()
        };

        let scope_spans = ScopeSpans {
            spans: vec![span],
            ..Default::default()
        };

        let request = TraceRequest {
            resource_spans: vec![ResourceSpans {
                scope_spans: vec![scope_spans],
                ..Default::default()
            }],
        };

        let bytes = request.encode_to_vec();
        let decoded = parse_otlp_trace_request(&bytes, InputFormat::Protobuf).unwrap();

        assert_eq!(decoded, request);
    }

    #[test]
    fn parses_json_trace_fixture() {
        let json_bytes = include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../testdata/trace.json"
        ));

        let mut json_value: JsonValue = serde_json::from_slice(json_bytes).unwrap();
        normalise_json_value(&mut json_value, None).unwrap();
        assert!(json_value["resource_spans"][0]["scope_spans"][0]["spans"][0]["flags"].is_number());
        let first_attr = &json_value["resource_spans"][0]["resource"]["attributes"][0];
        let attr: KeyValue = serde_json::from_value(first_attr.clone()).unwrap();
        assert_eq!(attr.key, semconv::SERVICE_NAME);
        let attr_value = attr
            .value
            .as_ref()
            .and_then(|value| any_value_string(value));
        assert_eq!(attr_value, Some("frontend-proxy"));

        let request = parse_otlp_trace_request(json_bytes, InputFormat::Json).unwrap();
        assert_eq!(request.resource_spans.len(), 1);

        let span_total: usize = request.resource_spans[0]
            .scope_spans
            .iter()
            .map(|scope| scope.spans.len())
            .sum();
        assert_eq!(span_total, 2);

        let has_service_name = request.resource_spans[0]
            .resource
            .as_ref()
            .map(|resource| {
                resource
                    .attributes
                    .iter()
                    .any(|attr| attr.key == semconv::SERVICE_NAME)
            })
            .unwrap_or(false);
        assert!(has_service_name, "service.name attribute missing");
        let parsed_service_name = request.resource_spans[0]
            .resource
            .as_ref()
            .and_then(|resource| {
                resource
                    .attributes
                    .iter()
                    .find(|attr| attr.key == semconv::SERVICE_NAME)
            })
            .and_then(|attr| attr.value.as_ref())
            .and_then(any_value_string)
            .unwrap();

        assert_eq!(parsed_service_name, "frontend-proxy");
    }

    #[test]
    fn parses_jsonl_trace_fixture() {
        let jsonl_bytes = include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../testdata/traces.jsonl"
        ));

        for (line_idx, line) in jsonl_bytes.split(|b| *b == b'\n').enumerate() {
            if line.is_empty() {
                continue;
            }
            let mut json_value: JsonValue = serde_json::from_slice(line).unwrap();
            normalise_json_value(&mut json_value, None).unwrap();
            assert!(
                json_value["resource_spans"][0]["scope_spans"][0]["spans"][0]["flags"].is_number(),
                "line {} missing flags",
                line_idx + 1
            );
        }

        let request = parse_otlp_trace_request(jsonl_bytes, InputFormat::Jsonl).unwrap();
        assert_eq!(request.resource_spans.len(), 19);

        let span_total: usize = request
            .resource_spans
            .iter()
            .map(|resource| {
                resource
                    .scope_spans
                    .iter()
                    .map(|scope| scope.spans.len())
                    .sum::<usize>()
            })
            .sum();
        assert_eq!(span_total, 19);

        let has_service_name = request.resource_spans.iter().any(|resource| {
            resource.resource.as_ref().is_some_and(|res| {
                res.attributes
                    .iter()
                    .any(|attr| attr.key == semconv::SERVICE_NAME)
            })
        });

        assert!(
            has_service_name,
            "expected at least one service.name attribute"
        );
    }

    #[test]
    fn parses_protobuf_trace_fixture() {
        let proto_bytes = include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../testdata/trace.pb"
        ));

        let request =
            parse_otlp_trace_request(proto_bytes, InputFormat::Protobuf).expect("protobuf decode");

        assert_eq!(request.resource_spans.len(), 1);
        let scope_spans = &request.resource_spans[0].scope_spans;
        assert_eq!(scope_spans.len(), 1);
        let span_total: usize = scope_spans.iter().map(|scope| scope.spans.len()).sum();
        assert_eq!(span_total, 2);

        let service_name = request.resource_spans[0]
            .resource
            .as_ref()
            .and_then(|resource| {
                resource
                    .attributes
                    .iter()
                    .find(|attr| attr.key == semconv::SERVICE_NAME)
            })
            .and_then(|attr| attr.value.as_ref())
            .and_then(any_value_string)
            .unwrap();

        assert_eq!(service_name, "frontend-proxy");
    }

    #[test]
    fn any_value_deserializes_from_canonical_shape() {
        let mut json: JsonValue = serde_json::from_str(r#"{"stringValue":"example"}"#).unwrap();
        normalise_json_value(&mut json, Some(otlp::VALUE)).unwrap();
        let any: AnyValue = serde_json::from_value(json).unwrap();
        assert!(matches!(
            any.value,
            Some(
                otlp2parquet_proto::opentelemetry::proto::common::v1::any_value::Value::StringValue(
                    _
                )
            )
        ));
    }

    #[test]
    fn any_value_serializes_to_expected_shape() {
        use otlp2parquet_proto::opentelemetry::proto::common::v1::{
            any_value::Value, AnyValue, KeyValue,
        };

        let kv = KeyValue {
            key: "service.name".into(),
            value: Some(AnyValue {
                value: Some(Value::StringValue("foo".into())),
            }),
        };
        let json = serde_json::to_string(&kv).unwrap();
        assert_eq!(
            json,
            r#"{"key":"service.name","value":{"value":{"StringValue":"foo"}}}"#
        );
    }

    #[test]
    fn key_value_deserializes_canonical_shape() {
        use otlp2parquet_proto::opentelemetry::proto::common::v1::{any_value::Value, KeyValue};

        let mut json: JsonValue =
            serde_json::from_str(r#"{"key":"service.name","value":{"stringValue":"foo"}}"#)
                .unwrap();
        normalise_json_value(&mut json, None).unwrap();
        let kv: KeyValue = serde_json::from_value(json).unwrap();

        assert!(matches!(
            kv.value.and_then(|v| v.value),
            Some(Value::StringValue(ref s)) if s == "foo"
        ));
    }
}

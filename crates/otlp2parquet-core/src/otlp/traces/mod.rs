mod format;
mod to_arrow;

pub use format::{parse_otlp_trace_request, TraceRequest};
pub use to_arrow::{TraceArrowConverter, TraceMetadata};

use crate::otlp::common::UNKNOWN_SERVICE_NAME;
use crate::otlp::common::{any_value_builder::any_value_string, field_names::semconv};
use otlp2parquet_proto::opentelemetry::proto::trace::v1::ResourceSpans;

/// Split a trace request into per-service chunks to preserve service-level routing.
pub fn split_request_by_service(request: TraceRequest) -> Vec<TraceRequest> {
    if request.resource_spans.len() <= 1 {
        return vec![request];
    }

    let mut groups: Vec<(String, Vec<ResourceSpans>)> = Vec::new();

    for resource_spans in request.resource_spans {
        let service_name = resource_spans
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
            .filter(|value| !value.is_empty())
            .map(String::from)
            .unwrap_or_else(|| UNKNOWN_SERVICE_NAME.to_string());

        if let Some((_, entries)) = groups
            .iter_mut()
            .find(|(name, _)| name.as_str() == service_name.as_str())
        {
            entries.push(resource_spans);
        } else {
            groups.push((service_name, vec![resource_spans]));
        }
    }

    groups
        .into_iter()
        .map(|(_, resource_spans)| TraceRequest { resource_spans })
        .collect()
}

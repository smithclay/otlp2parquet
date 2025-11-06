pub use crate::otlp::common::{field_names, field_numbers, InputFormat};

mod format;
pub mod to_arrow;

pub use format::parse_otlp_request;
pub use to_arrow::{ArrowConverter, MetricsMetadata};

use crate::otlp::common::UNKNOWN_SERVICE_NAME;
use crate::otlp::common::{any_value_builder::any_value_string, field_names::semconv};
use otlp2parquet_proto::opentelemetry::proto::collector::metrics::v1::ExportMetricsServiceRequest;
use otlp2parquet_proto::opentelemetry::proto::metrics::v1::ResourceMetrics;

/// Split an OTLP metrics request into one per `service.name`.
pub fn split_request_by_service(
    request: ExportMetricsServiceRequest,
) -> Vec<ExportMetricsServiceRequest> {
    if request.resource_metrics.len() <= 1 {
        return vec![request];
    }

    let mut groups: Vec<(String, Vec<ResourceMetrics>)> = Vec::new();

    for resource_metrics in request.resource_metrics {
        let service_name = resource_metrics
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
            entries.push(resource_metrics);
        } else {
            groups.push((service_name, vec![resource_metrics]));
        }
    }

    groups
        .into_iter()
        .map(|(_, resource_metrics)| ExportMetricsServiceRequest { resource_metrics })
        .collect()
}

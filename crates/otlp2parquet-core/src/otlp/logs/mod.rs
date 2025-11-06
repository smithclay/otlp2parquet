pub use crate::otlp::common::{field_names, field_numbers, InputFormat};

mod format;
pub mod to_arrow;

pub use format::parse_otlp_request;
pub use to_arrow::{ArrowConverter, LogMetadata};

use crate::otlp::common::UNKNOWN_SERVICE_NAME;
use crate::otlp::common::{any_value_builder::any_value_string, field_names::semconv};
use otlp2parquet_proto::opentelemetry::proto::{
    collector::logs::v1::ExportLogsServiceRequest, logs::v1::ResourceLogs,
};

/// Split an OTLP logs request into one request per `service.name`.
///
/// This preserves the original ordering of service groups while avoiding
/// mixing records from different services in downstream batching.
pub fn split_request_by_service(
    request: ExportLogsServiceRequest,
) -> Vec<ExportLogsServiceRequest> {
    if request.resource_logs.len() <= 1 {
        return vec![request];
    }

    let mut groups: Vec<(String, Vec<ResourceLogs>)> = Vec::new();

    for resource_logs in request.resource_logs {
        let service_name = resource_logs
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
            entries.push(resource_logs);
        } else {
            groups.push((service_name, vec![resource_logs]));
        }
    }

    groups
        .into_iter()
        .map(|(_, resource_logs)| ExportLogsServiceRequest { resource_logs })
        .collect()
}

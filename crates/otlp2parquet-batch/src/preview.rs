// Request preview and batch key calculation
//
// Extracts metadata from OTLP requests for batching decisions

use otlp2parquet_proto::opentelemetry::proto::{
    collector::logs::v1::ExportLogsServiceRequest, common::v1::any_value,
};
use prost::Message;

/// Batch key for grouping logs by service and time
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct BatchKey {
    pub service: String,
    pub hour_bucket: i64,
}

/// Quick preview of request metadata without full parsing
#[derive(Debug, Clone)]
pub(crate) struct RequestPreview {
    pub service_name: String,
    pub first_timestamp: i64,
    pub record_count: usize,
    pub encoded_len: usize,
}

impl RequestPreview {
    pub fn from_request(req: &ExportLogsServiceRequest) -> Self {
        let mut service_name = String::new();
        let mut first_ts = 0_i64;
        let mut record_count = 0_usize;

        for resource_logs in &req.resource_logs {
            if let Some(resource) = &resource_logs.resource {
                for attr in &resource.attributes {
                    if attr.key == "service.name" {
                        if let Some(any_value::Value::StringValue(value)) =
                            attr.value.as_ref().and_then(|v| v.value.as_ref())
                        {
                            service_name = value.clone();
                        }
                    }
                }
            }

            for scope_logs in &resource_logs.scope_logs {
                for record in &scope_logs.log_records {
                    record_count += 1;
                    let ts = record.time_unix_nano as i64;
                    if ts > 0 {
                        if first_ts == 0 {
                            first_ts = ts;
                        } else {
                            first_ts = first_ts.min(ts);
                        }
                    }
                }
            }
        }

        if service_name.is_empty() {
            service_name = "unknown".to_string();
        }

        Self {
            service_name,
            first_timestamp: first_ts,
            record_count,
            encoded_len: req.encoded_len(),
        }
    }

    pub fn batch_key(&self) -> BatchKey {
        let hour_bucket = if self.first_timestamp > 0 {
            self.first_timestamp / 3_600_000_000_000
        } else {
            0
        };

        BatchKey {
            service: self.service_name.clone(),
            hour_bucket,
        }
    }
}

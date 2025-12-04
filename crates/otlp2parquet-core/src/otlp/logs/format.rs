use anyhow::Result;
use otlp2parquet_proto::opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest;

use crate::otlp::common::{
    json_normalizer::normalise_json_value, parse_request, InputFormat, OtlpSignalRequest,
};

/// Parse OTLP logs from bytes in the specified format.
pub fn parse_otlp_request(bytes: &[u8], format: InputFormat) -> Result<ExportLogsServiceRequest> {
    parse_request(bytes, format, Some(normalise_json_value))
}

impl OtlpSignalRequest for ExportLogsServiceRequest {
    const JSONL_EMPTY_ERROR: &'static str = "JSONL input contained no valid log records";

    fn merge(&mut self, mut other: Self) {
        self.resource_logs.append(&mut other.resource_logs);
    }

    fn is_empty(&self) -> bool {
        self.resource_logs.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_empty_jsonl() {
        let result = parse_otlp_request(b"", InputFormat::Jsonl);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("no valid log records"));

        let result = parse_otlp_request(b"\n\n  \n", InputFormat::Jsonl);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_invalid_utf8_jsonl() {
        let invalid_utf8 = vec![0xFF, 0xFE, 0xFD];
        let result = parse_otlp_request(&invalid_utf8, InputFormat::Jsonl);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not valid UTF-8"));
    }

    #[test]
    fn test_parse_json_body() {
        let json = r#"{
            "resourceLogs": [{
                "scopeLogs": [{
                    "logRecords": [{
                        "body": { "stringValue": "test" }
                    }]
                }]
            }]
        }"#;

        let request = parse_otlp_request(json.as_bytes(), InputFormat::Json).expect("Failed to parse");
        let log = &request.resource_logs[0].scope_logs[0].log_records[0];
        let body = log.body.as_ref().expect("Body is None");

        if let Some(otlp2parquet_proto::opentelemetry::proto::common::v1::any_value::Value::StringValue(s)) = &body.value {
            assert_eq!(s, "test");
        } else {
            panic!("Body value is not StringValue: {:?}", body.value);
        }
    }

    #[test]
    fn test_parse_json_body_array() {
        let json = r#"{
            "resourceLogs": [{
                "scopeLogs": [{
                    "logRecords": [{
                        "body": { "arrayValue": { "values": [ { "stringValue": "test" } ] } }
                    }]
                }]
            }]
        }"#;

        let request = parse_otlp_request(json.as_bytes(), InputFormat::Json).expect("Failed to parse");
        let log = &request.resource_logs[0].scope_logs[0].log_records[0];
        let body = log.body.as_ref().expect("Body is None");

        if let Some(otlp2parquet_proto::opentelemetry::proto::common::v1::any_value::Value::ArrayValue(arr)) = &body.value {
             let item = &arr.values[0];
             if let Some(otlp2parquet_proto::opentelemetry::proto::common::v1::any_value::Value::StringValue(s)) = &item.value {
                 assert_eq!(s, "test");
             } else {
                 panic!("Array item value is not StringValue: {:?}", item.value);
             }
        } else {
            panic!("Body value is not ArrayValue");
        }
    }

    #[test]
    fn test_parse_json_kvlist_body() {
        let json = r#"{
            "resourceLogs": [{
                "scopeLogs": [{
                    "logRecords": [{
                        "body": { "kvlistValue": { "values": [ { "key": "k", "value": { "stringValue": "v" } } ] } }
                    }]
                }]
            }]
        }"#;

        let request = parse_otlp_request(json.as_bytes(), InputFormat::Json).expect("Failed to parse");
        let log = &request.resource_logs[0].scope_logs[0].log_records[0];
        let body = log.body.as_ref().expect("Body is None");

        if let Some(otlp2parquet_proto::opentelemetry::proto::common::v1::any_value::Value::KvlistValue(kv)) = &body.value {
             let item = &kv.values[0];
             assert_eq!(item.key, "k");
             // item.value is AnyValue
             if let Some(otlp2parquet_proto::opentelemetry::proto::common::v1::any_value::Value::StringValue(s)) = &item.value.as_ref().unwrap().value {
                 assert_eq!(s, "v");
             } else {
                 panic!("KV item value is not StringValue");
             }
        } else {
            panic!("Body value is not KvlistValue: {:?}", body.value);
        }
    }
}

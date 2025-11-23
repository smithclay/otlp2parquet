/// Core error classification for OTLP ingestion
#[derive(Debug, Clone)]
pub enum OtlpError {
    // 400-level: Client errors
    InvalidRequest {
        message: String,
        hint: Option<String>,
    },
    PayloadTooLarge {
        size: usize,
        limit: usize,
    },

    // 500-level: Server errors
    ConversionFailed {
        signal: String,
        message: String,
    },
    StorageFailed {
        message: String,
    },
    CatalogFailed {
        message: String,
    },
    InternalError {
        message: String,
    },
}

impl OtlpError {
    /// HTTP status code for this error
    pub fn status_code(&self) -> u16 {
        match self {
            Self::InvalidRequest { .. } => 400,
            Self::PayloadTooLarge { .. } => 413,
            Self::ConversionFailed { .. } => 500,
            Self::StorageFailed { .. } => 502,
            Self::CatalogFailed { .. } => 502,
            Self::InternalError { .. } => 500,
        }
    }

    /// Error type string for responses
    pub fn error_type(&self) -> &'static str {
        match self {
            Self::InvalidRequest { .. } => "InvalidRequest",
            Self::PayloadTooLarge { .. } => "PayloadTooLarge",
            Self::ConversionFailed { .. } => "ConversionFailed",
            Self::StorageFailed { .. } => "StorageFailed",
            Self::CatalogFailed { .. } => "CatalogFailed",
            Self::InternalError { .. } => "InternalError",
        }
    }

    /// Human-readable message
    pub fn message(&self) -> String {
        match self {
            Self::InvalidRequest { message, .. } => message.clone(),
            Self::PayloadTooLarge { size, limit } => format!(
                "Payload size {} bytes exceeds limit of {} bytes",
                size, limit
            ),
            Self::ConversionFailed { signal, message } => {
                format!("Failed to convert {} to Arrow: {}", signal, message)
            }
            Self::StorageFailed { message } => format!("Storage operation failed: {}", message),
            Self::CatalogFailed { message } => format!("Catalog operation failed: {}", message),
            Self::InternalError { message } => message.clone(),
        }
    }

    /// Optional hint for fixing the error
    pub fn hint(&self) -> Option<String> {
        match self {
            Self::InvalidRequest { hint, .. } => hint.clone(),
            Self::PayloadTooLarge { .. } => {
                Some("Reduce batch size or increase OTLP2PARQUET_MAX_PAYLOAD_BYTES".into())
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_status_codes() {
        let err = OtlpError::InvalidRequest {
            message: "test".into(),
            hint: None,
        };
        assert_eq!(err.status_code(), 400);

        let err = OtlpError::PayloadTooLarge {
            size: 1000,
            limit: 500,
        };
        assert_eq!(err.status_code(), 413);

        let err = OtlpError::ConversionFailed {
            signal: "logs".into(),
            message: "failed".into(),
        };
        assert_eq!(err.status_code(), 500);

        let err = OtlpError::StorageFailed {
            message: "failed".into(),
        };
        assert_eq!(err.status_code(), 502);

        let err = OtlpError::CatalogFailed {
            message: "failed".into(),
        };
        assert_eq!(err.status_code(), 502);

        let err = OtlpError::InternalError {
            message: "failed".into(),
        };
        assert_eq!(err.status_code(), 500);
    }

    #[test]
    fn test_error_types() {
        let err = OtlpError::InvalidRequest {
            message: "test".into(),
            hint: None,
        };
        assert_eq!(err.error_type(), "InvalidRequest");

        let err = OtlpError::PayloadTooLarge {
            size: 1000,
            limit: 500,
        };
        assert_eq!(err.error_type(), "PayloadTooLarge");

        let err = OtlpError::ConversionFailed {
            signal: "logs".into(),
            message: "failed".into(),
        };
        assert_eq!(err.error_type(), "ConversionFailed");

        let err = OtlpError::StorageFailed {
            message: "failed".into(),
        };
        assert_eq!(err.error_type(), "StorageFailed");
    }

    #[test]
    fn test_error_messages() {
        let err = OtlpError::InvalidRequest {
            message: "bad data".into(),
            hint: Some("check format".into()),
        };
        assert_eq!(err.message(), "bad data");

        let err = OtlpError::PayloadTooLarge {
            size: 1000,
            limit: 500,
        };
        assert!(err.message().contains("1000"));
        assert!(err.message().contains("500"));

        let err = OtlpError::ConversionFailed {
            signal: "logs".into(),
            message: "schema mismatch".into(),
        };
        assert!(err.message().contains("logs"));
        assert!(err.message().contains("schema mismatch"));
    }

    #[test]
    fn test_error_hints() {
        let err = OtlpError::InvalidRequest {
            message: "bad data".into(),
            hint: Some("check format".into()),
        };
        assert_eq!(err.hint(), Some("check format".to_string()));

        let err = OtlpError::PayloadTooLarge {
            size: 1000,
            limit: 500,
        };
        let hint = err.hint().unwrap();
        assert!(hint.contains("OTLP2PARQUET_MAX_PAYLOAD_BYTES"));

        let err = OtlpError::StorageFailed {
            message: "failed".into(),
        };
        assert_eq!(err.hint(), None);
    }
}

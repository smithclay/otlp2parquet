// Error handling and HTTP status code mapping for Cloudflare Workers
//
// Provides consistent error responses with descriptive messages and appropriate HTTP status codes

use serde::Serialize;
use worker::Response;

/// Error classification with HTTP status code mapping
#[derive(Debug)]
#[non_exhaustive]
pub enum OtlpErrorKind {
    // 400-level: Client errors
    /// 400 - Malformed OTLP data, parsing failures
    InvalidRequest(String),
    /// 401 - Auth failures
    Unauthorized(String),
    /// 413 - Request body exceeds limit
    PayloadTooLarge(String),

    // 500-level: Server errors
    /// 500 - Misconfiguration
    ConfigError(ConfigValidationError),
    /// 502 - R2/storage backend issues
    StorageError(String),
}

impl OtlpErrorKind {
    /// Get HTTP status code for this error kind
    pub fn status_code(&self) -> u16 {
        match self {
            Self::InvalidRequest(_) => 400,
            Self::Unauthorized(_) => 401,
            Self::PayloadTooLarge(_) => 413,
            Self::ConfigError(_) => 500,
            Self::StorageError(_) => 502,
        }
    }

    /// Get error type string for JSON response
    pub fn error_type(&self) -> &'static str {
        match self {
            Self::InvalidRequest(_) => "InvalidRequest",
            Self::Unauthorized(_) => "Unauthorized",
            Self::PayloadTooLarge(_) => "PayloadTooLarge",
            Self::ConfigError(_) => "ConfigError",
            Self::StorageError(_) => "StorageError",
        }
    }

    /// Get human-readable error message
    pub fn message(&self) -> String {
        match self {
            Self::InvalidRequest(msg)
            | Self::Unauthorized(msg)
            | Self::PayloadTooLarge(msg)
            | Self::StorageError(msg) => msg.clone(),
            Self::ConfigError(err) => err.message(),
        }
    }

    /// Get detailed error information (optional)
    pub fn details(&self) -> Option<String> {
        match self {
            Self::ConfigError(err) => Some(err.details()),
            _ => None,
        }
    }
}

/// Configuration validation errors with specific diagnostic messages
#[derive(Debug)]
#[non_exhaustive]
pub enum ConfigValidationError {
    /// Some required variables are missing
    MissingRequired {
        component: &'static str,
        missing_vars: Vec<&'static str>,
        hint: String,
    },
    /// Catalog configuration is partially present
    PartialCatalogConfig {
        catalog_type: &'static str,
        present: Vec<&'static str>,
        missing: Vec<&'static str>,
        hint: String,
    },
    /// Environment variable has invalid value
    InvalidValue {
        var_name: &'static str,
        value: String,
        expected: &'static str,
    },
}

impl ConfigValidationError {
    pub fn message(&self) -> String {
        match self {
            Self::MissingRequired { component, .. } => {
                format!("{} configuration incomplete", component)
            }
            Self::PartialCatalogConfig { catalog_type, .. } => {
                format!("{} is partially configured", catalog_type)
            }
            Self::InvalidValue { var_name, .. } => {
                format!("Invalid configuration value for {}", var_name)
            }
        }
    }

    pub fn details(&self) -> String {
        match self {
            Self::MissingRequired {
                missing_vars, hint, ..
            } => {
                format!(
                    "Required environment variables not set: {}. {}",
                    missing_vars.join(", "),
                    hint
                )
            }
            Self::PartialCatalogConfig {
                present,
                missing,
                hint,
                ..
            } => {
                format!(
                    "Found {} but missing {}. {}",
                    present.join(", "),
                    missing.join(", "),
                    hint
                )
            }
            Self::InvalidValue {
                var_name,
                value,
                expected,
            } => {
                format!(
                    "{} has value '{}' but expected {}",
                    var_name, value, expected
                )
            }
        }
    }
}

/// Standardized JSON error response
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
}

impl ErrorResponse {
    /// Create error response from OtlpErrorKind
    pub fn from_error(error: OtlpErrorKind, request_id: Option<String>) -> Self {
        Self {
            error: error.error_type().to_string(),
            message: error.message(),
            details: error.details(),
            request_id,
        }
    }

    /// Convert into worker::Response with appropriate status code
    pub fn into_response(self, status_code: u16) -> worker::Result<Response> {
        Ok(Response::from_json(&self)?.with_status(status_code))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_status_codes() {
        assert_eq!(
            OtlpErrorKind::InvalidRequest("test".into()).status_code(),
            400
        );
        assert_eq!(
            OtlpErrorKind::Unauthorized("test".into()).status_code(),
            401
        );
        assert_eq!(
            OtlpErrorKind::PayloadTooLarge("test".into()).status_code(),
            413
        );
        assert_eq!(
            OtlpErrorKind::ConfigError(ConfigValidationError::MissingRequired {
                component: "Test",
                missing_vars: vec!["VAR"],
                hint: "fix".into()
            })
            .status_code(),
            500
        );
        assert_eq!(
            OtlpErrorKind::StorageError("test".into()).status_code(),
            502
        );
    }

    #[test]
    fn test_config_error_messages() {
        let err = ConfigValidationError::MissingRequired {
            component: "R2 Storage",
            missing_vars: vec!["VAR1", "VAR2"],
            hint: "Set these variables".into(),
        };
        assert_eq!(err.message(), "R2 Storage configuration incomplete");
        assert!(err.details().contains("VAR1, VAR2"));
    }

    #[test]
    fn test_partial_catalog_config_message() {
        let err = ConfigValidationError::PartialCatalogConfig {
            catalog_type: "R2 Data Catalog",
            present: vec!["CLOUDFLARE_ACCOUNT_ID"],
            missing: vec!["CLOUDFLARE_BUCKET_NAME", "CLOUDFLARE_API_TOKEN"],
            hint: "Provide all three variables".into(),
        };
        assert_eq!(err.message(), "R2 Data Catalog is partially configured");
        assert!(err.details().contains("CLOUDFLARE_ACCOUNT_ID"));
        assert!(err.details().contains("CLOUDFLARE_BUCKET_NAME"));
    }

    #[test]
    fn test_invalid_value_message() {
        let err = ConfigValidationError::InvalidValue {
            var_name: "OTLP2PARQUET_BASIC_AUTH_ENABLED",
            value: "yes".into(),
            expected: "'true' or 'false'",
        };
        assert!(err.message().contains("Invalid configuration value"));
        assert!(err.details().contains("'yes'"));
        assert!(err.details().contains("'true' or 'false'"));
    }

    #[test]
    fn test_error_response_serialization() {
        let response = ErrorResponse {
            error: "ConfigError".into(),
            message: "Something went wrong".into(),
            details: Some("Detailed info".into()),
            request_id: Some("req_123".into()),
        };

        let json = serde_json::to_value(&response).unwrap();
        assert_eq!(json["error"], "ConfigError");
        assert_eq!(json["message"], "Something went wrong");
        assert_eq!(json["details"], "Detailed info");
        assert_eq!(json["request_id"], "req_123");
    }

    #[test]
    fn test_error_response_omits_none_fields() {
        let response = ErrorResponse {
            error: "InvalidRequest".into(),
            message: "Bad data".into(),
            details: None,
            request_id: None,
        };

        let json = serde_json::to_value(&response).unwrap();
        assert!(!json.as_object().unwrap().contains_key("details"));
        assert!(!json.as_object().unwrap().contains_key("request_id"));
    }
}

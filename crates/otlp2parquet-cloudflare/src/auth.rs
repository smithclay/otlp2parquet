// Basic authentication for Cloudflare Workers
//
// Optional HTTP Basic Auth that can be enabled via environment variables

use subtle::ConstantTimeEq;
use worker::{Env, Request, Response};

use crate::errors;

/// Parse and validate Basic Auth header
///
/// Returns Ok((username, password)) if valid, Err otherwise
pub fn parse_basic_auth_header(
    auth_header: &str,
) -> std::result::Result<(String, String), &'static str> {
    // Parse "Basic <base64>" format
    let auth_value = auth_header.trim();
    if !auth_value.starts_with("Basic ") {
        return Err("Invalid auth header format");
    }

    let encoded = &auth_value[6..]; // Skip "Basic "

    // Decode base64
    use base64::Engine;
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(|_| "Invalid base64 encoding")?;

    let credentials = String::from_utf8(decoded).map_err(|_| "Invalid UTF-8 in credentials")?;

    // Split on first colon: username:password
    let parts: Vec<&str> = credentials.splitn(2, ':').collect();
    if parts.len() != 2 {
        return Err("Invalid credentials format");
    }

    Ok((parts[0].to_string(), parts[1].to_string()))
}

/// Validate credentials against expected values using constant-time comparison.
///
/// This prevents timing attacks by ensuring both username and password are
/// always compared in full, regardless of where they differ.
pub fn validate_credentials(
    username: &str,
    password: &str,
    expected_user: &str,
    expected_pass: &str,
) -> bool {
    // Constant-time comparison to prevent timing attacks.
    // Both comparisons always execute fully regardless of intermediate results.
    let user_match = username.as_bytes().ct_eq(expected_user.as_bytes());
    let pass_match = password.as_bytes().ct_eq(expected_pass.as_bytes());
    (user_match & pass_match).into()
}

/// Check basic authentication if enabled via environment variables
///
/// Returns Ok(()) if auth passes or is disabled, Err(Response) with 401 if auth fails
pub fn check_basic_auth(
    req: &Request,
    env: &Env,
    request_id: Option<&str>,
) -> std::result::Result<(), Response> {
    let request_id = request_id.map(|s| s.to_string());

    // Check if basic auth is enabled
    let auth_var = env.var("OTLP2PARQUET_BASIC_AUTH_ENABLED").ok();
    let auth_enabled = match auth_var {
        None => false,
        Some(v) => {
            let value = v.to_string();
            match value.parse::<bool>() {
                Ok(b) => b,
                Err(_) => {
                    // Invalid value like "yes", "1", etc.
                    let error = errors::OtlpErrorKind::ConfigError(
                        errors::ConfigValidationError::InvalidValue {
                            var_name: "OTLP2PARQUET_BASIC_AUTH_ENABLED",
                            value,
                            expected: "'true' or 'false'",
                        },
                    );
                    let status_code = error.status_code();
                    let error_response =
                        errors::ErrorResponse::from_error(error, request_id.clone());
                    return Err(error_response
                        .into_response(status_code)
                        .unwrap_or_else(|_| {
                            Response::error("Invalid configuration value", status_code).unwrap()
                        }));
                }
            }
        }
    };

    if !auth_enabled {
        return Ok(());
    }

    // Get expected credentials from env vars
    let expected_username = env
        .var("OTLP2PARQUET_BASIC_AUTH_USERNAME")
        .ok()
        .map(|v| v.to_string());

    let expected_password = env
        .secret("OTLP2PARQUET_BASIC_AUTH_PASSWORD")
        .or_else(|_| env.var("OTLP2PARQUET_BASIC_AUTH_PASSWORD"))
        .ok()
        .map(|v| v.to_string());

    // If auth is enabled but credentials not configured, return config error
    let (Some(expected_user), Some(expected_pass)) = (expected_username, expected_password) else {
        tracing::error!(
            request_id = ?request_id,
            "Basic auth enabled but credentials not configured"
        );

        let error = errors::OtlpErrorKind::ConfigError(errors::ConfigValidationError::MissingRequired {
            component: "Authentication",
            missing_vars: vec!["OTLP2PARQUET_BASIC_AUTH_USERNAME", "OTLP2PARQUET_BASIC_AUTH_PASSWORD"],
            hint: "OTLP2PARQUET_BASIC_AUTH_ENABLED is 'true' but username and password are not set. Either disable auth (set to 'false') or provide both username and password.".to_string(),
        });
        let status_code = error.status_code();
        let error_response = errors::ErrorResponse::from_error(error, request_id);
        // into_response only fails if JSON serialization fails, which is extremely rare
        // In that case, return a simple text error response
        return Err(error_response
            .into_response(status_code)
            .unwrap_or_else(|_| {
                Response::error("Authentication configuration error", status_code).unwrap()
            }));
    };

    // Get Authorization header
    let auth_header = match req.headers().get("Authorization").ok().flatten() {
        Some(h) => h,
        None => {
            let error = errors::OtlpErrorKind::Unauthorized(
                "Authentication required. Provide Basic authentication credentials.".to_string(),
            );
            let status_code = error.status_code();
            let error_response = errors::ErrorResponse::from_error(error, request_id);
            let mut response = error_response
                .into_response(status_code)
                .unwrap_or_else(|_| {
                    Response::error("Authentication required", status_code).unwrap()
                });
            response = response.with_headers(worker::Headers::from_iter([(
                "WWW-Authenticate",
                "Basic realm=\"OTLP Ingestion\"",
            )]));
            return Err(response);
        }
    };

    // Parse and validate credentials
    let (username, password) = match parse_basic_auth_header(&auth_header) {
        Ok(creds) => creds,
        Err(e) => {
            tracing::warn!(request_id = ?request_id, error = %e, "Failed to parse auth header");

            let error = errors::OtlpErrorKind::Unauthorized(format!(
                "Invalid authentication format: {}. Expected 'Basic <base64>' header.",
                e
            ));
            let status_code = error.status_code();
            let error_response = errors::ErrorResponse::from_error(error, request_id);
            return Err(error_response
                .into_response(status_code)
                .unwrap_or_else(|_| {
                    Response::error("Invalid authentication format", status_code).unwrap()
                }));
        }
    };

    if validate_credentials(&username, &password, &expected_user, &expected_pass) {
        Ok(())
    } else {
        tracing::warn!(request_id = ?request_id, "Invalid credentials provided");

        let error =
            errors::OtlpErrorKind::Unauthorized("Invalid username or password.".to_string());
        let status_code = error.status_code();
        let error_response = errors::ErrorResponse::from_error(error, request_id);
        Err(error_response
            .into_response(status_code)
            .unwrap_or_else(|_| Response::error("Invalid credentials", status_code).unwrap()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_basic_auth_valid() {
        // "user:pass" in base64 is "dXNlcjpwYXNz"
        let header = "Basic dXNlcjpwYXNz";
        let result = parse_basic_auth_header(header);
        assert!(result.is_ok());
        let (username, password) = result.unwrap();
        assert_eq!(username, "user");
        assert_eq!(password, "pass");
    }

    #[test]
    fn test_parse_basic_auth_with_colon_in_password() {
        // "admin:my:complex:pass" in base64 is "YWRtaW46bXk6Y29tcGxleDpwYXNz"
        let header = "Basic YWRtaW46bXk6Y29tcGxleDpwYXNz";
        let result = parse_basic_auth_header(header);
        assert!(result.is_ok());
        let (username, password) = result.unwrap();
        assert_eq!(username, "admin");
        assert_eq!(password, "my:complex:pass");
    }

    #[test]
    fn test_parse_basic_auth_with_whitespace() {
        // "user:pass" with leading/trailing whitespace
        let header = "  Basic dXNlcjpwYXNz  ";
        let result = parse_basic_auth_header(header);
        assert!(result.is_ok());
        let (username, password) = result.unwrap();
        assert_eq!(username, "user");
        assert_eq!(password, "pass");
    }

    #[test]
    fn test_parse_basic_auth_missing_basic_prefix() {
        let header = "dXNlcjpwYXNz"; // Missing "Basic " prefix
        let result = parse_basic_auth_header(header);
        assert!(result.is_err());
        assert_eq!(result.err().unwrap(), "Invalid auth header format");
    }

    #[test]
    fn test_parse_basic_auth_invalid_base64() {
        let header = "Basic not-valid-base64!!!";
        let result = parse_basic_auth_header(header);
        assert!(result.is_err());
        assert_eq!(result.err().unwrap(), "Invalid base64 encoding");
    }

    #[test]
    fn test_parse_basic_auth_missing_colon() {
        // "userpass" (no colon) in base64 is "dXNlcnBhc3M="
        let header = "Basic dXNlcnBhc3M=";
        let result = parse_basic_auth_header(header);
        assert!(result.is_err());
        assert_eq!(result.err().unwrap(), "Invalid credentials format");
    }

    #[test]
    fn test_parse_basic_auth_empty_credentials() {
        // ":" in base64 is "Og=="
        let header = "Basic Og==";
        let result = parse_basic_auth_header(header);
        assert!(result.is_ok());
        let (username, password) = result.unwrap();
        assert_eq!(username, "");
        assert_eq!(password, "");
    }

    #[test]
    fn test_validate_credentials_correct() {
        assert!(validate_credentials("user", "pass", "user", "pass"));
    }

    #[test]
    fn test_validate_credentials_wrong_username() {
        assert!(!validate_credentials("wrong", "pass", "user", "pass"));
    }

    #[test]
    fn test_validate_credentials_wrong_password() {
        assert!(!validate_credentials("user", "wrong", "user", "pass"));
    }

    #[test]
    fn test_validate_credentials_case_sensitive() {
        assert!(!validate_credentials("User", "pass", "user", "pass"));
        assert!(!validate_credentials("user", "Pass", "user", "pass"));
    }

    #[test]
    fn test_validate_credentials_empty() {
        assert!(validate_credentials("", "", "", ""));
    }

    #[test]
    fn test_validate_credentials_special_chars() {
        let user = "admin@example.com";
        let pass = "p@$$w0rd!#$%";
        assert!(validate_credentials(user, pass, user, pass));
    }
}

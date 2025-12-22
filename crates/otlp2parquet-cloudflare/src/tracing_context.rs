//! W3C Trace Context propagation for Cloudflare Workers.
//!
//! This module provides TraceContext for generating and propagating distributed tracing
//! headers across Worker -> DO -> Worker hops. It follows the W3C Trace Context spec
//! (<https://www.w3.org/TR/trace-context/>) and generates request IDs for correlation.

use uuid::Uuid;

/// TraceContext holds distributed tracing identifiers for a request.
///
/// Fields:
/// - `request_id`: UUID v4 string (with hyphens) for request correlation
/// - `trace_id`: 32 hex chars (UUID without hyphens) for distributed trace
/// - `span_id`: 16 hex chars (first 16 chars of UUID without hyphens) for current span
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceContext {
    pub request_id: String,
    pub trace_id: String,
    pub span_id: String,
}

impl TraceContext {
    /// Create a new TraceContext with generated IDs.
    ///
    /// Generates:
    /// - `request_id`: UUID v4 with hyphens (e.g., "550e8400-e29b-41d4-a716-446655440000")
    /// - `trace_id`: 32 hex chars from UUID (e.g., "550e8400e29b41d4a716446655440000")
    /// - `span_id`: 16 hex chars from first half of new UUID (e.g., "550e8400e29b41d4")
    pub fn new() -> Self {
        let request_uuid = Uuid::new_v4();
        let trace_uuid = Uuid::new_v4();
        let span_uuid = Uuid::new_v4();

        let request_id = request_uuid.to_string(); // With hyphens
        let trace_id = trace_uuid.simple().to_string(); // No hyphens, 32 chars
        let span_id = span_uuid.simple().to_string()[..16].to_string(); // First 16 chars

        Self {
            request_id,
            trace_id,
            span_id,
        }
    }

    /// Format as W3C traceparent header value.
    ///
    /// Format: `00-{trace_id}-{span_id}-01`
    /// - `00`: version
    /// - `{trace_id}`: 32 hex chars
    /// - `{span_id}`: 16 hex chars
    /// - `01`: trace flags (sampled)
    ///
    /// # Examples
    ///
    /// ```
    /// # use otlp2parquet_cloudflare::TraceContext;
    /// let ctx = TraceContext::new();
    /// let header = ctx.traceparent();
    /// assert!(header.starts_with("00-"));
    /// assert!(header.ends_with("-01"));
    /// ```
    pub fn traceparent(&self) -> String {
        format!("00-{}-{}-01", self.trace_id, self.span_id)
    }

    /// Create a child TraceContext with the same request_id and trace_id but a new span_id.
    ///
    /// This is used to create nested spans in the same trace when propagating context
    /// through Worker -> DO or DO -> Worker hops.
    ///
    /// # Examples
    ///
    /// ```
    /// # use otlp2parquet_cloudflare::TraceContext;
    /// let parent = TraceContext::new();
    /// let child = parent.child();
    ///
    /// assert_eq!(parent.request_id, child.request_id);
    /// assert_eq!(parent.trace_id, child.trace_id);
    /// assert_ne!(parent.span_id, child.span_id);
    /// ```
    pub fn child(&self) -> Self {
        let span_uuid = Uuid::new_v4();
        let span_id = span_uuid.simple().to_string()[..16].to_string();

        Self {
            request_id: self.request_id.clone(),
            trace_id: self.trace_id.clone(),
            span_id,
        }
    }

    /// Parse TraceContext from incoming request headers.
    ///
    /// Looks for:
    /// - `X-Request-Id`: If present, used as request_id (otherwise generated)
    /// - `traceparent`: W3C format `00-{trace_id}-{span_id}-{flags}` (otherwise generated)
    ///
    /// If headers are missing or invalid, falls back to generating new IDs.
    ///
    /// # Examples
    ///
    /// ```
    /// # use otlp2parquet_cloudflare::TraceContext;
    /// # use std::collections::HashMap;
    /// let mut headers = HashMap::new();
    /// headers.insert("x-request-id".to_string(), "550e8400-e29b-41d4-a716-446655440000".to_string());
    /// headers.insert("traceparent".to_string(), "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string());
    ///
    /// let ctx = TraceContext::from_headers(|name| headers.get(name).map(|s| s.as_str()));
    ///
    /// assert_eq!(ctx.request_id, "550e8400-e29b-41d4-a716-446655440000");
    /// assert_eq!(ctx.trace_id, "0af7651916cd43dd8448eb211c80319c");
    /// assert_eq!(ctx.span_id, "b7ad6b7169203331");
    /// ```
    pub fn from_headers<'a, F>(get_header: F) -> Self
    where
        F: Fn(&str) -> Option<&'a str>,
    {
        // Try to get request_id from X-Request-Id header (case-insensitive)
        let request_id = get_header("x-request-id")
            .or_else(|| get_header("X-Request-Id"))
            .map(|s| s.to_string())
            .unwrap_or_else(|| Uuid::new_v4().to_string());

        // Try to parse traceparent header
        if let Some(traceparent) = get_header("traceparent").or_else(|| get_header("Traceparent")) {
            if let Some((trace_id, span_id)) = Self::parse_traceparent(traceparent) {
                return Self {
                    request_id,
                    trace_id,
                    span_id,
                };
            }
        }

        // Fallback: generate new trace and span IDs
        let trace_uuid = Uuid::new_v4();
        let span_uuid = Uuid::new_v4();

        Self {
            request_id,
            trace_id: trace_uuid.simple().to_string(),
            span_id: span_uuid.simple().to_string()[..16].to_string(),
        }
    }

    /// Parse W3C traceparent header value.
    ///
    /// Expected format: `00-{trace_id}-{span_id}-{flags}`
    /// Returns Some((trace_id, span_id)) if valid, None otherwise.
    fn parse_traceparent(header: &str) -> Option<(String, String)> {
        let parts: Vec<&str> = header.split('-').collect();
        if parts.len() != 4 {
            return None;
        }

        // Validate version (00)
        if parts[0] != "00" {
            return None;
        }

        // Validate trace_id (32 hex chars)
        if parts[1].len() != 32 || !parts[1].chars().all(|c| c.is_ascii_hexdigit()) {
            return None;
        }

        // Validate span_id (16 hex chars)
        if parts[2].len() != 16 || !parts[2].chars().all(|c| c.is_ascii_hexdigit()) {
            return None;
        }

        // Validate flags (2 hex chars)
        if parts[3].len() != 2 || !parts[3].chars().all(|c| c.is_ascii_hexdigit()) {
            return None;
        }

        Some((parts[1].to_string(), parts[2].to_string()))
    }
}

impl Default for TraceContext {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_new_generates_valid_ids() {
        let ctx = TraceContext::new();

        // request_id should be UUID with hyphens (36 chars)
        assert_eq!(ctx.request_id.len(), 36);
        assert!(ctx.request_id.contains('-'));
        assert!(Uuid::parse_str(&ctx.request_id).is_ok());

        // trace_id should be 32 hex chars
        assert_eq!(ctx.trace_id.len(), 32);
        assert!(ctx.trace_id.chars().all(|c| c.is_ascii_hexdigit()));

        // span_id should be 16 hex chars
        assert_eq!(ctx.span_id.len(), 16);
        assert!(ctx.span_id.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_traceparent_format() {
        let ctx = TraceContext {
            request_id: "550e8400-e29b-41d4-a716-446655440000".to_string(),
            trace_id: "0af7651916cd43dd8448eb211c80319c".to_string(),
            span_id: "b7ad6b7169203331".to_string(),
        };

        let header = ctx.traceparent();
        assert_eq!(
            header,
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
        );
    }

    #[test]
    fn test_child_preserves_trace_and_request() {
        let parent = TraceContext::new();
        let child = parent.child();

        assert_eq!(parent.request_id, child.request_id);
        assert_eq!(parent.trace_id, child.trace_id);
        assert_ne!(parent.span_id, child.span_id);

        // Child span_id should still be valid 16 hex chars
        assert_eq!(child.span_id.len(), 16);
        assert!(child.span_id.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_default_trait() {
        let ctx = TraceContext::default();
        assert_eq!(ctx.request_id.len(), 36);
        assert_eq!(ctx.trace_id.len(), 32);
        assert_eq!(ctx.span_id.len(), 16);
    }

    #[test]
    fn test_from_headers_with_valid_headers() {
        let mut headers = HashMap::new();
        headers.insert(
            "x-request-id".to_string(),
            "550e8400-e29b-41d4-a716-446655440000".to_string(),
        );
        headers.insert(
            "traceparent".to_string(),
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string(),
        );

        let ctx = TraceContext::from_headers(|name| headers.get(name).map(|s| s.as_str()));

        assert_eq!(ctx.request_id, "550e8400-e29b-41d4-a716-446655440000");
        assert_eq!(ctx.trace_id, "0af7651916cd43dd8448eb211c80319c");
        assert_eq!(ctx.span_id, "b7ad6b7169203331");
    }

    #[test]
    fn test_from_headers_case_insensitive() {
        let mut headers = HashMap::new();
        headers.insert(
            "X-Request-Id".to_string(),
            "550e8400-e29b-41d4-a716-446655440000".to_string(),
        );
        headers.insert(
            "Traceparent".to_string(),
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string(),
        );

        let ctx = TraceContext::from_headers(|name| headers.get(name).map(|s| s.as_str()));

        assert_eq!(ctx.request_id, "550e8400-e29b-41d4-a716-446655440000");
        assert_eq!(ctx.trace_id, "0af7651916cd43dd8448eb211c80319c");
        assert_eq!(ctx.span_id, "b7ad6b7169203331");
    }

    #[test]
    fn test_from_headers_missing_headers() {
        let headers: HashMap<String, String> = HashMap::new();
        let ctx = TraceContext::from_headers(|name| headers.get(name).map(|s| s.as_str()));

        // Should generate new IDs
        assert_eq!(ctx.request_id.len(), 36);
        assert_eq!(ctx.trace_id.len(), 32);
        assert_eq!(ctx.span_id.len(), 16);
    }

    #[test]
    fn test_from_headers_partial_headers() {
        let mut headers = HashMap::new();
        headers.insert(
            "x-request-id".to_string(),
            "550e8400-e29b-41d4-a716-446655440000".to_string(),
        );
        // No traceparent header

        let ctx = TraceContext::from_headers(|name| headers.get(name).map(|s| s.as_str()));

        // Should use provided request_id but generate trace/span
        assert_eq!(ctx.request_id, "550e8400-e29b-41d4-a716-446655440000");
        assert_eq!(ctx.trace_id.len(), 32);
        assert_eq!(ctx.span_id.len(), 16);
    }

    #[test]
    fn test_parse_traceparent_valid() {
        let result = TraceContext::parse_traceparent(
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
        );
        assert!(result.is_some());
        let (trace_id, span_id) = result.unwrap();
        assert_eq!(trace_id, "0af7651916cd43dd8448eb211c80319c");
        assert_eq!(span_id, "b7ad6b7169203331");
    }

    #[test]
    fn test_parse_traceparent_invalid_version() {
        let result = TraceContext::parse_traceparent(
            "01-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
        );
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_traceparent_invalid_trace_id_length() {
        let result = TraceContext::parse_traceparent("00-0af7651916cd43dd-b7ad6b7169203331-01");
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_traceparent_invalid_span_id_length() {
        let result =
            TraceContext::parse_traceparent("00-0af7651916cd43dd8448eb211c80319c-b7ad6b71-01");
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_traceparent_invalid_hex() {
        let result = TraceContext::parse_traceparent(
            "00-0af7651916cd43dd8448eb211c80319g-b7ad6b7169203331-01",
        );
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_traceparent_wrong_parts() {
        let result = TraceContext::parse_traceparent("00-0af7651916cd43dd8448eb211c80319c-01");
        assert!(result.is_none());
    }

    #[test]
    fn test_multiple_children_have_different_span_ids() {
        let parent = TraceContext::new();
        let child1 = parent.child();
        let child2 = parent.child();

        assert_eq!(child1.request_id, child2.request_id);
        assert_eq!(child1.trace_id, child2.trace_id);
        assert_ne!(child1.span_id, child2.span_id);
    }

    #[test]
    fn test_traceparent_roundtrip() {
        let ctx1 = TraceContext::new();
        let traceparent = ctx1.traceparent();

        // Parse it back
        let (trace_id, span_id) = TraceContext::parse_traceparent(&traceparent).unwrap();
        assert_eq!(trace_id, ctx1.trace_id);
        assert_eq!(span_id, ctx1.span_id);
    }
}

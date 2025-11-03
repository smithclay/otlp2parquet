// HTTP response builders for Lambda
//
// Converts internal response format to Lambda-specific response types

use aws_lambda_events::{
    apigw::{ApiGatewayProxyRequest, ApiGatewayProxyResponse},
    encodings::Body,
    http::{header::CONTENT_TYPE, HeaderValue},
    lambda_function_urls::{LambdaFunctionUrlRequest, LambdaFunctionUrlResponse},
};
use serde::{Deserialize, Serialize};

/// Internal HTTP response data
pub(crate) struct HttpResponseData {
    pub status_code: u16,
    pub body: String,
    pub content_type: &'static str,
}

impl HttpResponseData {
    pub fn json(status_code: u16, body: String) -> Self {
        Self {
            status_code,
            body,
            content_type: "application/json",
        }
    }

    pub fn text(status_code: u16, body: String) -> Self {
        Self {
            status_code,
            body,
            content_type: "text/plain; charset=utf-8",
        }
    }
}

/// Lambda event types (API Gateway or Function URL)
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub(crate) enum HttpRequestEvent {
    ApiGateway(Box<ApiGatewayProxyRequest>),
    FunctionUrl(Box<LambdaFunctionUrlRequest>),
}

/// Lambda response types
#[derive(Debug, Serialize)]
#[serde(untagged)]
pub(crate) enum HttpLambdaResponse {
    ApiGateway(ApiGatewayProxyResponse),
    FunctionUrl(LambdaFunctionUrlResponse),
}

/// Build API Gateway response from internal response data
pub(crate) fn build_api_gateway_response(data: HttpResponseData) -> HttpLambdaResponse {
    let mut response = ApiGatewayProxyResponse {
        status_code: data.status_code as i64,
        headers: Default::default(),
        multi_value_headers: Default::default(),
        body: Some(Body::Text(data.body)),
        is_base64_encoded: false,
    };
    response
        .headers
        .insert(CONTENT_TYPE, HeaderValue::from_static(data.content_type));
    HttpLambdaResponse::ApiGateway(response)
}

/// Build Function URL response from internal response data
pub(crate) fn build_function_url_response(data: HttpResponseData) -> HttpLambdaResponse {
    let mut headers = aws_lambda_events::http::HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static(data.content_type));
    HttpLambdaResponse::FunctionUrl(LambdaFunctionUrlResponse {
        status_code: data.status_code as i64,
        headers,
        body: Some(data.body),
        is_base64_encoded: false,
        cookies: Vec::new(),
    })
}

//! Connect command - generates configuration for external services

mod url;

use anyhow::Result;
use clap::{Args, Subcommand};

pub use url::resolve_endpoint_url;

#[derive(Subcommand)]
pub enum ConnectCommand {
    /// Generate OpenTelemetry Collector configuration
    OtelCollector(OtelCollectorArgs),
    /// Generate Claude Code configuration
    ClaudeCode(ClaudeCodeArgs),
    /// Generate OpenAI Codex CLI configuration
    Codex(CodexArgs),
}

impl ConnectCommand {
    pub async fn run(self) -> Result<()> {
        match self {
            ConnectCommand::OtelCollector(args) => execute_otel_collector(args).await,
            ConnectCommand::ClaudeCode(args) => execute_claude_code(args).await,
            ConnectCommand::Codex(args) => execute_codex(args).await,
        }
    }
}

#[derive(Args)]
pub struct OtelCollectorArgs {
    /// OTLP endpoint URL (default: http://localhost:4318)
    #[arg(long)]
    pub url: Option<String>,
}

#[derive(Args)]
pub struct ClaudeCodeArgs {
    /// OTLP endpoint URL (default: http://localhost:4318)
    #[arg(long)]
    pub url: Option<String>,

    /// Output format: shell (default) or json
    #[arg(long, default_value = "shell")]
    pub format: String,
}

#[derive(Args)]
pub struct CodexArgs {
    /// OTLP endpoint URL (default: http://localhost:4318)
    #[arg(long)]
    pub url: Option<String>,
}

/// Generate OpenTelemetry Collector configuration
async fn execute_otel_collector(args: OtelCollectorArgs) -> Result<()> {
    let url = resolve_endpoint_url(args.url.as_deref())?;

    let config = generate_collector_config(&url);
    println!("{}", config);

    Ok(())
}

/// Generate Claude Code shell exports
async fn execute_claude_code(args: ClaudeCodeArgs) -> Result<()> {
    let url = resolve_endpoint_url(args.url.as_deref())?;

    let output = generate_claude_code_config(&url, &args.format);
    println!("{}", output);

    Ok(())
}

/// Generate OpenAI Codex CLI configuration
async fn execute_codex(args: CodexArgs) -> Result<()> {
    let url = resolve_endpoint_url(args.url.as_deref())?;

    let config = generate_codex_config(&url);
    println!("{}", config);

    Ok(())
}

fn generate_collector_config(endpoint: &str) -> String {
    format!(
        r#"# OpenTelemetry Collector configuration for otlp2parquet
# Save as otel-collector-config.yaml and run:
#   otelcol --config otel-collector-config.yaml

receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
      http:
        endpoint: 0.0.0.0:4318

processors:
  batch:
    # Batch by resource (includes service.name) to reduce requests
    send_batch_size: 1000
    send_batch_max_size: 2000
    timeout: 5s

exporters:
  otlphttp:
    endpoint: {endpoint}
    compression: gzip

service:
  pipelines:
    logs:
      receivers: [otlp]
      processors: [batch]
      exporters: [otlphttp]
    traces:
      receivers: [otlp]
      processors: [batch]
      exporters: [otlphttp]
    metrics:
      receivers: [otlp]
      processors: [batch]
      exporters: [otlphttp]
"#,
        endpoint = endpoint
    )
}

fn generate_claude_code_config(endpoint: &str, format: &str) -> String {
    match format {
        "json" => generate_claude_code_json(endpoint),
        _ => generate_claude_code_shell(endpoint),
    }
}

fn generate_claude_code_shell(endpoint: &str) -> String {
    format!(
        r#"# Claude Code OpenTelemetry configuration for otlp2parquet
# Add to your shell profile (~/.bashrc, ~/.zshrc) or run before starting claude:

export CLAUDE_CODE_ENABLE_TELEMETRY=1
export OTEL_METRICS_EXPORTER=otlp
export OTEL_LOGS_EXPORTER=otlp
export OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
export OTEL_EXPORTER_OTLP_ENDPOINT={endpoint}

# Optional: reduce export intervals for faster updates (in milliseconds)
# export OTEL_METRIC_EXPORT_INTERVAL=10000
# export OTEL_LOGS_EXPORT_INTERVAL=5000

# Optional: include user prompts in logs (disabled by default for privacy)
# export OTEL_LOG_USER_PROMPTS=1

# Optional: add team/department labels for cost attribution
# export OTEL_RESOURCE_ATTRIBUTES=department=engineering,team.id=platform
"#,
        endpoint = endpoint
    )
}

fn generate_claude_code_json(endpoint: &str) -> String {
    format!(
        r#"# Claude Code settings.json for otlp2parquet
# Add to your settings file: ~/.claude/settings.json

{{
  "env": {{
    "CLAUDE_CODE_ENABLE_TELEMETRY": "1",
    "OTEL_METRICS_EXPORTER": "otlp",
    "OTEL_LOGS_EXPORTER": "otlp",
    "OTEL_EXPORTER_OTLP_PROTOCOL": "http/protobuf",
    "OTEL_EXPORTER_OTLP_ENDPOINT": "{endpoint}"
  }}
}}"#,
        endpoint = endpoint
    )
}

fn generate_codex_config(endpoint: &str) -> String {
    format!(
        r#"# OpenAI Codex CLI configuration for otlp2parquet
# Add to your codex config file (~/.codex/config.toml)
# or run: codex config --edit

[otel]
# Enable OTLP HTTP exporter for logs
exporter = "otlp-http"

# Optional: also enable trace export
# trace_exporter = "otlp-http"

# Optional: log user prompts (disabled by default for privacy)
# log_user_prompt = true

# Optional: environment label for filtering
# environment = "dev"

[otel.exporter."otlp-http"]
endpoint = "{endpoint}/v1/logs"
protocol = "binary"

# Optional: add auth header if needed
# [otel.exporter."otlp-http".headers]
# "Authorization" = "Bearer ${{AUTH_TOKEN}}"
"#,
        endpoint = endpoint
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_collector_config() {
        let config = generate_collector_config("https://example.com");
        assert!(config.contains("endpoint: https://example.com"));
        assert!(config.contains("compression: gzip"));
        assert!(config.contains("processors: [batch]"));
        assert!(config.contains("send_batch_size: 1000"));
        // All three signal types should be present
        assert!(config.contains("logs:"));
        assert!(config.contains("traces:"));
        assert!(config.contains("metrics:"));
    }

    #[test]
    fn test_generate_claude_code_shell() {
        let config = generate_claude_code_shell("https://example.com");
        assert!(config.contains("CLAUDE_CODE_ENABLE_TELEMETRY=1"));
        assert!(config.contains("OTEL_METRICS_EXPORTER=otlp"));
        assert!(config.contains("OTEL_LOGS_EXPORTER=otlp"));
        assert!(config.contains("OTEL_EXPORTER_OTLP_ENDPOINT=https://example.com"));
        assert!(config.contains("http/protobuf"));
    }

    #[test]
    fn test_generate_claude_code_json() {
        let config = generate_claude_code_json("https://example.com");
        assert!(config.contains("\"CLAUDE_CODE_ENABLE_TELEMETRY\": \"1\""));
        assert!(config.contains("\"OTEL_EXPORTER_OTLP_ENDPOINT\": \"https://example.com\""));
        // Should include file path instructions
        assert!(config.contains("settings.json"));
        assert!(config.contains("~/.claude/settings.json"));
    }

    #[test]
    fn test_generate_codex_config() {
        let config = generate_codex_config("https://example.com");
        assert!(config.contains("[otel]"));
        assert!(config.contains("exporter = \"otlp-http\""));
        assert!(config.contains("[otel.exporter.\"otlp-http\"]"));
        assert!(config.contains("endpoint = \"https://example.com/v1/logs\""));
        assert!(config.contains("protocol = \"binary\""));
        // Should include config file path instructions
        assert!(config.contains("~/.codex/config.toml"));
    }
}

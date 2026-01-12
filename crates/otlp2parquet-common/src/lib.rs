//! Shared types and configuration for otlp2parquet.

pub mod config;
pub mod types;

pub use config::{
    BatchConfig, CloudflareConfig, EnvSource, FsConfig, LambdaConfig, LogFormat, Platform,
    RequestConfig, RuntimeConfig, ServerConfig, StorageBackend, StorageConfig, ENV_PREFIX,
};
pub use types::{Blake3Hash, MetricType, SignalKey, SignalType};

// Re-export from otlp2records
pub use otlp2records::InputFormat;

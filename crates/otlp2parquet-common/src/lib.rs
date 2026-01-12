//! Shared types and configuration for otlp2parquet.

pub mod config;
pub mod types;

pub use config::{
    BatchConfig, CloudflareConfig, EnvSource, FsConfig, LambdaConfig, LogFormat, Platform,
    RequestConfig, RuntimeConfig, ServerConfig, StorageBackend, StorageConfig, ENV_PREFIX,
};
pub use types::{Blake3Hash, MetricType, ParquetWriteResult, SignalKey, SignalType};

// Re-export from otlp2records
pub use otlp2records::{
    count_skipped_metric_data_points, normalise_json_value, normalize_json_bytes, InputFormat,
    MetricSkipCounts,
};

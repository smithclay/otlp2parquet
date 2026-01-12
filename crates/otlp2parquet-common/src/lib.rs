//! Shared types and configuration for otlp2parquet.

pub mod config;
pub mod input_format;
pub mod otlp_json;
pub mod types;

pub use config::{
    BatchConfig, CloudflareConfig, EnvSource, FsConfig, LambdaConfig, LogFormat, Platform,
    RequestConfig, RuntimeConfig, ServerConfig, StorageBackend, StorageConfig, ENV_PREFIX,
};
pub use input_format::InputFormat;
pub use otlp_json::{
    count_skipped_metric_data_points, normalise_json_value, normalize_json_bytes, MetricSkipCounts,
};
pub use types::{Blake3Hash, MetricType, ParquetWriteResult, SignalKey, SignalType};

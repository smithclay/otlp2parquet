use std::env;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use aws_credential_types::Credentials;
use aws_sigv4::http_request::{
    sign, SignableBody, SignableRequest, SigningParams, SigningSettings,
};
use aws_sigv4::sign::v4;
use aws_smithy_runtime_api::client::identity::Identity;
use chrono::{DateTime, Utc};
use iceberg::catalog::NamespaceIdent;
use iceberg::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, Schema, Struct,
    UnboundPartitionSpec,
};
use parquet::file::metadata::ParquetMetaData;
use reqwest::{Method, Url};
use serde::{Deserialize, Serialize};
use tracing::{debug, instrument, warn};

use crate::parquet_writer::ParquetWriteResult;

/// Configuration block describing an Iceberg table managed by S3 Tables.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct S3TablesConfig {
    pub catalog_type: String,
    pub rest_uri: String,
    pub warehouse: String,
    pub namespace: Vec<String>,
    pub table: String,
    #[serde(default = "default_format_version")]
    pub format_version: i32,
    #[serde(default = "default_target_file_size_bytes")]
    pub target_file_size_bytes: u64,
    #[serde(default = "default_staging_prefix")]
    pub staging_prefix: String,
}

const DEFAULT_CATALOG_TYPE: &str = "s3tables";
const DEFAULT_STAGING_PREFIX: &str = "data/incoming";
const DEFAULT_TARGET_FILE_SIZE: u64 = 512 * 1024 * 1024;

fn default_format_version() -> i32 {
    2
}

fn default_target_file_size_bytes() -> u64 {
    DEFAULT_TARGET_FILE_SIZE
}

fn default_staging_prefix() -> String {
    DEFAULT_STAGING_PREFIX.to_string()
}

impl Default for S3TablesConfig {
    fn default() -> Self {
        Self {
            catalog_type: DEFAULT_CATALOG_TYPE.to_string(),
            rest_uri: String::new(),
            warehouse: String::new(),
            namespace: Vec::new(),
            table: String::new(),
            format_version: default_format_version(),
            target_file_size_bytes: default_target_file_size_bytes(),
            staging_prefix: default_staging_prefix(),
        }
    }
}

impl S3TablesConfig {
    /// Load configuration from environment variables.
    pub fn from_env() -> Result<Self> {
        let rest_uri = env::var("ICEBERG_REST_URI")
            .context("ICEBERG_REST_URI must be set for S3 Tables catalog access")?;
        let warehouse = env::var("ICEBERG_WAREHOUSE")
            .context("ICEBERG_WAREHOUSE must be set for S3 Tables catalog access")?;
        let namespace = env::var("ICEBERG_NAMESPACE")
            .unwrap_or_default()
            .split('.')
            .filter(|segment| !segment.trim().is_empty())
            .map(|segment| segment.trim().to_string())
            .collect::<Vec<_>>();
        let table = env::var("ICEBERG_TABLE")
            .context("ICEBERG_TABLE must be set for S3 Tables catalog access")?;

        let catalog_type =
            env::var("ICEBERG_CATALOG_TYPE").unwrap_or_else(|_| DEFAULT_CATALOG_TYPE.to_string());

        let staging_prefix = env::var("ICEBERG_STAGING_PREFIX")
            .unwrap_or_else(|_| DEFAULT_STAGING_PREFIX.to_string());

        let target_file_size_bytes = env::var("ICEBERG_TARGET_FILE_SIZE_BYTES")
            .ok()
            .and_then(|raw| raw.parse::<u64>().ok())
            .unwrap_or(DEFAULT_TARGET_FILE_SIZE);

        Ok(Self {
            catalog_type,
            rest_uri,
            warehouse,
            namespace,
            table,
            format_version: default_format_version(),
            target_file_size_bytes,
            staging_prefix,
        })
    }

    pub fn namespace_ident(&self) -> Result<NamespaceIdent> {
        if self.namespace.is_empty() {
            bail!("Iceberg namespace must contain at least one element");
        }

        NamespaceIdent::from_strs(self.namespace.clone())
            .context("failed to parse namespace for Iceberg table")
    }
}

/// Thin wrapper describing a fully-qualified Iceberg table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IcebergTableIdentifier {
    pub namespace: NamespaceIdent,
    pub name: String,
}

impl IcebergTableIdentifier {
    pub fn new(namespace: NamespaceIdent, name: impl Into<String>) -> Self {
        Self {
            namespace,
            name: name.into(),
        }
    }
}

/// Credentials resolved from the execution environment for SigV4 signing.
#[derive(Debug, Clone)]
pub struct AwsCredentials {
    pub identity: Identity,
    pub region: String,
}

impl AwsCredentials {
    pub fn from_env() -> Result<Self> {
        let access_key = env::var("AWS_ACCESS_KEY_ID")
            .context("AWS_ACCESS_KEY_ID must be set for S3 Tables access")?;
        let secret_key = env::var("AWS_SECRET_ACCESS_KEY")
            .context("AWS_SECRET_ACCESS_KEY must be set for S3 Tables access")?;
        let session_token = env::var("AWS_SESSION_TOKEN").ok();
        let region = env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string());

        let identity = Identity::new(Credentials::new(
            access_key,
            secret_key,
            session_token,
            None,
            "environment",
        ));

        Ok(Self { identity, region })
    }
}

/// Abstract catalog interactions required by the storage layer.
#[async_trait]
pub trait CatalogAdapter: Send + Sync {
    async fn ensure_namespace(&self, namespace: &NamespaceIdent) -> Result<()>;
    async fn ensure_table(
        &self,
        ident: &IcebergTableIdentifier,
        schema: Arc<Schema>,
        partition_spec: Arc<UnboundPartitionSpec>,
    ) -> Result<()>;
    async fn append_files(
        &self,
        ident: &IcebergTableIdentifier,
        files: Vec<DataFile>,
        committed_at: DateTime<Utc>,
    ) -> Result<()>;
    async fn expire_snapshots(
        &self,
        ident: &IcebergTableIdentifier,
        older_than: DateTime<Utc>,
        retain_last: usize,
    ) -> Result<()>;
}

/// Minimal S3 Tables REST client that signs requests with SigV4.
#[derive(Debug, Clone)]
pub struct S3TablesCatalog {
    client: reqwest::Client,
    config: S3TablesConfig,
    credentials: AwsCredentials,
}

impl S3TablesCatalog {
    pub fn new(config: S3TablesConfig, credentials: AwsCredentials) -> Result<Self> {
        let client = reqwest::Client::builder()
            .build()
            .context("failed to build reqwest client for S3 Tables catalog")?;

        Ok(Self {
            client,
            config,
            credentials,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn signed_request(
        &self,
        method: Method,
        url: Url,
        body: Option<Vec<u8>>,
    ) -> Result<reqwest::Response> {
        let mut request_builder = self.client.request(method.clone(), url.clone());
        if let Some(ref payload) = body {
            request_builder = request_builder.body(payload.clone());
        }

        let mut request = request_builder.build()?;

        let signable = SignableRequest::new(
            method.as_str(),
            url.as_str(),
            request
                .headers()
                .iter()
                .map(|(name, value)| (name.as_str(), value.to_str().unwrap_or_default())),
            match body {
                Some(ref payload) if !payload.is_empty() => SignableBody::Bytes(payload.as_slice()),
                _ => SignableBody::Bytes(&[]),
            },
        )?;

        let signing_settings = SigningSettings::default();
        let signing_params = v4::SigningParams::builder()
            .identity(&self.credentials.identity)
            .region(&self.credentials.region)
            .name("glue")
            .time(std::time::SystemTime::now())
            .settings(signing_settings)
            .build()
            .context("failed to build SigV4 signing params")?
            .into();

        let (instructions, _signature) = sign(signable, &signing_params)?;

        for (header_name, header_value) in instructions.headers() {
            request.headers_mut().insert(
                header_name,
                header_value
                    .parse()
                    .context("failed to parse signed header value")?,
            );
        }

        if !instructions.params().is_empty() {
            warn!("query parameter signing unsupported for S3 Tables requests");
        }

        let response = self.client.execute(request).await?;
        Ok(response)
    }
}

#[async_trait]
impl CatalogAdapter for S3TablesCatalog {
    #[instrument(skip_all)]
    async fn ensure_namespace(&self, namespace: &NamespaceIdent) -> Result<()> {
        debug!(namespace = ?namespace, "ensure_namespace noop");
        Ok(())
    }

    #[instrument(skip_all)]
    async fn ensure_table(
        &self,
        ident: &IcebergTableIdentifier,
        _schema: Arc<Schema>,
        _partition_spec: Arc<UnboundPartitionSpec>,
    ) -> Result<()> {
        debug!(table = %ident.name, namespace = ?ident.namespace, "ensure_table noop");
        Ok(())
    }

    #[instrument(skip_all)]
    async fn append_files(
        &self,
        _ident: &IcebergTableIdentifier,
        _files: Vec<DataFile>,
        _committed_at: DateTime<Utc>,
    ) -> Result<()> {
        bail!("S3 Tables commit path not yet implemented");
    }

    #[instrument(skip_all)]
    async fn expire_snapshots(
        &self,
        ident: &IcebergTableIdentifier,
        older_than: DateTime<Utc>,
        retain_last: usize,
    ) -> Result<()> {
        debug!(
            table = %ident.name,
            namespace = ?ident.namespace,
            older_than = %older_than,
            retain_last,
            "expire_snapshots noop"
        );
        Ok(())
    }
}

/// Convert a written Parquet file into an Iceberg DataFile descriptor.
pub fn build_data_file(
    result: &ParquetWriteResult,
    _table_config: &S3TablesConfig,
    _schema: &Schema,
    _partition_spec: &UnboundPartitionSpec,
) -> Result<DataFile> {
    let _parquet_metadata: &ParquetMetaData = &result.parquet_metadata;
    debug!(
        path = %result.path,
        row_count = result.row_count,
        file_size = result.file_size,
        "constructing iceberg DataFile (stub)"
    );

    let mut builder = DataFileBuilder::default();
    builder
        .file_path(result.path.clone())
        .file_format(DataFileFormat::Parquet)
        .record_count(result.row_count as u64)
        .file_size_in_bytes(result.file_size)
        .partition(Struct::empty())
        .content(DataContentType::Data)
        .partition_spec_id(0);

    builder
        .build()
        .context("failed to build Iceberg DataFile placeholder")
}

/// Placeholder committer wiring together Parquet outputs and catalog operations.
pub struct IcebergCommitter<A: CatalogAdapter> {
    adapter: Arc<A>,
    table_ident: IcebergTableIdentifier,
    schema: Arc<Schema>,
    partition_spec: Arc<UnboundPartitionSpec>,
}

impl<A: CatalogAdapter> IcebergCommitter<A> {
    pub fn new(
        adapter: Arc<A>,
        table_ident: IcebergTableIdentifier,
        schema: Arc<Schema>,
        partition_spec: Arc<UnboundPartitionSpec>,
    ) -> Self {
        Self {
            adapter,
            table_ident,
            schema,
            partition_spec,
        }
    }

    #[allow(dead_code)]
    pub async fn commit(&self, parquet_results: &[ParquetWriteResult]) -> Result<()> {
        let mut data_files = Vec::with_capacity(parquet_results.len());
        for result in parquet_results {
            let file = build_data_file(
                result,
                &S3TablesConfig::default(),
                &self.schema,
                &self.partition_spec,
            )?;
            data_files.push(file);
        }

        self.adapter
            .append_files(&self.table_ident, data_files, Utc::now())
            .await
    }
}

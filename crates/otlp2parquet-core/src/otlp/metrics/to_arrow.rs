// OTLP Metrics to Arrow conversion
//
// This module handles converting OTLP metrics data to Arrow RecordBatches
// with separate schemas for each metric type (gauge, sum, histogram, etc.)

use std::borrow::Cow;
use std::collections::HashMap;
use std::mem;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{
    Array, BooleanBuilder, Float64Builder, GenericListArray, Int32Builder, Int64Builder,
    ListBuilder, OffsetSizeTrait, RecordBatch, StringBuilder, TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Field};
use otlp2parquet_proto::opentelemetry::proto::{
    collector::metrics::v1::ExportMetricsServiceRequest,
    common::v1::KeyValue,
    metrics::v1::{
        metric::Data, number_data_point::Value, Metric, NumberDataPoint, ResourceMetrics,
        ScopeMetrics,
    },
};

use crate::otlp::common::{
    any_value_builder::any_value_string, field_names::semconv, keyvalue_to_json,
    string_pairs_to_json, UNKNOWN_SERVICE_NAME,
};
use crate::schema::metrics::*;

/// Helper to convert a ListArray from a ListBuilder to use a specific field
///
/// ListBuilder creates lists with default fields, but our schema requires
/// fields with specific metadata (field_id) for tooling compatibility. This function
/// reconstructs the array with the correct field definition from the schema.
fn list_array_with_field<OffsetSize: OffsetSizeTrait>(
    list_array: GenericListArray<OffsetSize>,
    field: Field,
) -> GenericListArray<OffsetSize> {
    let values = list_array.values().clone();
    let offsets = list_array.offsets().clone();
    let nulls = list_array.nulls().cloned();

    GenericListArray::new(Arc::new(field), offsets, values, nulls)
}

/// Metadata extracted from metrics request
#[derive(Debug, Clone, Default)]
pub struct MetricsMetadata {
    /// Number of resource metrics processed
    pub resource_metrics_count: usize,
    /// Number of scope metrics processed
    pub scope_metrics_count: usize,
    /// Number of data points processed by type
    pub gauge_count: usize,
    pub sum_count: usize,
    pub histogram_count: usize,
    pub exponential_histogram_count: usize,
    pub summary_count: usize,
    /// First timestamp per metric type (in microseconds)
    #[allow(clippy::derivable_impls)]
    pub metric_first_timestamps: HashMap<String, i64>,
    /// Service name associated with the converted batches
    pub service_name: String,
}

impl MetricsMetadata {
    /// Effective service name, falling back to UNKNOWN_SERVICE_NAME.
    pub fn service_name(&self) -> &str {
        if self.service_name.is_empty() {
            UNKNOWN_SERVICE_NAME
        } else {
            &self.service_name
        }
    }

    /// Retrieve the first timestamp for a given metric type (microseconds).
    pub fn first_timestamp_for(&self, metric_type: &str) -> Option<i64> {
        self.metric_first_timestamps.get(metric_type).copied()
    }

    fn observe_service_name(&mut self, candidate: &str) {
        if self.service_name.is_empty() && !candidate.is_empty() {
            self.service_name = candidate.to_string();
        }
    }

    fn finalize_service_name(&mut self) {
        if self.service_name.is_empty() {
            self.service_name = UNKNOWN_SERVICE_NAME.to_string();
        }
    }

    fn record_metric_timestamp(&mut self, metric_type: &str, timestamp: Option<i64>) {
        if let Some(ts) = timestamp {
            self.metric_first_timestamps
                .entry(metric_type.to_string())
                .and_modify(|existing| {
                    if ts < *existing {
                        *existing = ts;
                    }
                })
                .or_insert(ts);
        }
    }
}

/// Arrow converter for OTLP metrics data
///
/// Converts OTLP metrics to Arrow RecordBatches with separate batches
/// for each metric type.
pub struct ArrowConverter {
    schema_gauge: Arc<arrow::datatypes::Schema>,
    schema_sum: Arc<arrow::datatypes::Schema>,
    schema_histogram: Arc<arrow::datatypes::Schema>,
    schema_exponential_histogram: Arc<arrow::datatypes::Schema>,
    schema_summary: Arc<arrow::datatypes::Schema>,
}

// Upper bound to cap memory when ingesting very large metric payloads.
const METRIC_MAX_ROWS_PER_BATCH: usize = 65_536;
const METRIC_DEFAULT_BUILDER_CAPACITY: usize = 1024;

impl Default for ArrowConverter {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrowConverter {
    /// Create a new Arrow converter with all metric schemas
    pub fn new() -> Self {
        Self {
            schema_gauge: otel_metrics_gauge_schema_arc(),
            schema_sum: otel_metrics_sum_schema_arc(),
            schema_histogram: otel_metrics_histogram_schema_arc(),
            schema_exponential_histogram: otel_metrics_exponential_histogram_schema_arc(),
            schema_summary: otel_metrics_summary_schema_arc(),
        }
    }

    /// Convert OTLP metrics request to Arrow RecordBatches by type
    ///
    /// Returns a tuple of (batches_by_type, metadata) where batches_by_type
    /// is a vector of (metric_type_name, RecordBatch) tuples.
    pub fn convert(
        &self,
        request: ExportMetricsServiceRequest,
    ) -> Result<(Vec<(String, RecordBatch)>, MetricsMetadata)> {
        let mut metadata = MetricsMetadata {
            resource_metrics_count: request.resource_metrics.len(),
            ..Default::default()
        };

        // Collect data points by metric type
        let mut gauge_builder = GaugeBuilder::new();
        let mut sum_builder = SumBuilder::new();
        let mut histogram_builder = HistogramBuilder::new();
        let mut exp_histogram_builder = ExponentialHistogramBuilder::new();
        let mut summary_builder = SummaryBuilder::new();
        let mut batches = Vec::new();

        // Process all resource metrics
        for resource_metrics in &request.resource_metrics {
            let resource_ctx = extract_resource_context(resource_metrics);
            metadata.observe_service_name(&resource_ctx.service_name);

            for scope_metrics in &resource_metrics.scope_metrics {
                metadata.scope_metrics_count += 1;
                let scope_ctx = extract_scope_context(scope_metrics);

                for metric in &scope_metrics.metrics {
                    self.process_metric(
                        metric,
                        &resource_ctx,
                        &scope_ctx,
                        &mut gauge_builder,
                        &mut sum_builder,
                        &mut histogram_builder,
                        &mut exp_histogram_builder,
                        &mut summary_builder,
                        &mut metadata,
                        &mut batches,
                    )?;
                }
            }
        }

        metadata.finalize_service_name();

        // Flush any remaining rows for each metric type
        flush_gauge_builder(
            &mut gauge_builder,
            Arc::clone(&self.schema_gauge),
            &mut metadata,
            &mut batches,
        )?;
        flush_sum_builder(
            &mut sum_builder,
            Arc::clone(&self.schema_sum),
            &mut metadata,
            &mut batches,
        )?;
        flush_histogram_builder(
            &mut histogram_builder,
            Arc::clone(&self.schema_histogram),
            &mut metadata,
            &mut batches,
        )?;
        flush_exp_histogram_builder(
            &mut exp_histogram_builder,
            Arc::clone(&self.schema_exponential_histogram),
            &mut metadata,
            &mut batches,
        )?;
        flush_summary_builder(
            &mut summary_builder,
            Arc::clone(&self.schema_summary),
            &mut metadata,
            &mut batches,
        )?;

        Ok((batches, metadata))
    }

    #[allow(clippy::too_many_arguments)]
    fn process_metric(
        &self,
        metric: &Metric,
        resource_ctx: &ResourceContext,
        scope_ctx: &ScopeContext,
        gauge_builder: &mut GaugeBuilder,
        sum_builder: &mut SumBuilder,
        histogram_builder: &mut HistogramBuilder,
        exp_histogram_builder: &mut ExponentialHistogramBuilder,
        summary_builder: &mut SummaryBuilder,
        metadata: &mut MetricsMetadata,
        batches: &mut Vec<(String, RecordBatch)>,
    ) -> Result<()> {
        let data = metric.data.as_ref().context("Metric has no data")?;

        match data {
            Data::Gauge(gauge) => {
                for point in &gauge.data_points {
                    gauge_builder.add_data_point(metric, point, resource_ctx, scope_ctx)?;
                    if gauge_builder.len() >= METRIC_MAX_ROWS_PER_BATCH {
                        flush_gauge_builder(
                            gauge_builder,
                            Arc::clone(&self.schema_gauge),
                            metadata,
                            batches,
                        )?;
                    }
                }
            }
            Data::Sum(sum) => {
                for point in &sum.data_points {
                    sum_builder.add_data_point(
                        metric,
                        point,
                        sum.aggregation_temporality,
                        sum.is_monotonic,
                        resource_ctx,
                        scope_ctx,
                    )?;
                    if sum_builder.len() >= METRIC_MAX_ROWS_PER_BATCH {
                        flush_sum_builder(
                            sum_builder,
                            Arc::clone(&self.schema_sum),
                            metadata,
                            batches,
                        )?;
                    }
                }
            }
            Data::Histogram(histogram) => {
                for point in &histogram.data_points {
                    histogram_builder.add_data_point(metric, point, resource_ctx, scope_ctx)?;
                    if histogram_builder.len() >= METRIC_MAX_ROWS_PER_BATCH {
                        flush_histogram_builder(
                            histogram_builder,
                            Arc::clone(&self.schema_histogram),
                            metadata,
                            batches,
                        )?;
                    }
                }
            }
            Data::ExponentialHistogram(exp_histogram) => {
                for point in &exp_histogram.data_points {
                    exp_histogram_builder.add_data_point(metric, point, resource_ctx, scope_ctx)?;
                    if exp_histogram_builder.len() >= METRIC_MAX_ROWS_PER_BATCH {
                        flush_exp_histogram_builder(
                            exp_histogram_builder,
                            Arc::clone(&self.schema_exponential_histogram),
                            metadata,
                            batches,
                        )?;
                    }
                }
            }
            Data::Summary(summary) => {
                for point in &summary.data_points {
                    summary_builder.add_data_point(metric, point, resource_ctx, scope_ctx)?;
                    if summary_builder.len() >= METRIC_MAX_ROWS_PER_BATCH {
                        flush_summary_builder(
                            summary_builder,
                            Arc::clone(&self.schema_summary),
                            metadata,
                            batches,
                        )?;
                    }
                }
            }
        }

        Ok(())
    }

    /// Get the schema for a specific metric type
    pub fn schema_for_type(&self, metric_type: &str) -> Option<Arc<arrow::datatypes::Schema>> {
        match metric_type {
            "gauge" => Some(Arc::clone(&self.schema_gauge)),
            "sum" => Some(Arc::clone(&self.schema_sum)),
            "histogram" => Some(Arc::clone(&self.schema_histogram)),
            "exponential_histogram" => Some(Arc::clone(&self.schema_exponential_histogram)),
            "summary" => Some(Arc::clone(&self.schema_summary)),
            _ => None,
        }
    }
}

fn flush_gauge_builder(
    builder: &mut GaugeBuilder,
    schema: Arc<arrow::datatypes::Schema>,
    metadata: &mut MetricsMetadata,
    batches: &mut Vec<(String, RecordBatch)>,
) -> Result<()> {
    if builder.len() == 0 {
        return Ok(());
    }

    let current = mem::replace(
        builder,
        GaugeBuilder::with_capacity(METRIC_MAX_ROWS_PER_BATCH),
    );
    let len = current.len();
    metadata.gauge_count += len;
    metadata.record_metric_timestamp("gauge", current.first_timestamp());
    let batch = current.finish(schema)?;
    batches.push(("gauge".to_string(), batch));
    Ok(())
}

fn flush_sum_builder(
    builder: &mut SumBuilder,
    schema: Arc<arrow::datatypes::Schema>,
    metadata: &mut MetricsMetadata,
    batches: &mut Vec<(String, RecordBatch)>,
) -> Result<()> {
    if builder.len() == 0 {
        return Ok(());
    }

    let current = mem::replace(
        builder,
        SumBuilder::with_capacity(METRIC_MAX_ROWS_PER_BATCH),
    );
    let len = current.len();
    metadata.sum_count += len;
    metadata.record_metric_timestamp("sum", current.first_timestamp());
    let batch = current.finish(schema)?;
    batches.push(("sum".to_string(), batch));
    Ok(())
}

fn flush_histogram_builder(
    builder: &mut HistogramBuilder,
    schema: Arc<arrow::datatypes::Schema>,
    metadata: &mut MetricsMetadata,
    batches: &mut Vec<(String, RecordBatch)>,
) -> Result<()> {
    if builder.len() == 0 {
        return Ok(());
    }

    let current = mem::replace(
        builder,
        HistogramBuilder::with_capacity(METRIC_MAX_ROWS_PER_BATCH),
    );
    let len = current.len();
    metadata.histogram_count += len;
    metadata.record_metric_timestamp("histogram", current.first_timestamp());
    let batch = current.finish(schema)?;
    batches.push(("histogram".to_string(), batch));
    Ok(())
}

fn flush_exp_histogram_builder(
    builder: &mut ExponentialHistogramBuilder,
    schema: Arc<arrow::datatypes::Schema>,
    metadata: &mut MetricsMetadata,
    batches: &mut Vec<(String, RecordBatch)>,
) -> Result<()> {
    if builder.len() == 0 {
        return Ok(());
    }

    let current = mem::replace(
        builder,
        ExponentialHistogramBuilder::with_capacity(METRIC_MAX_ROWS_PER_BATCH),
    );
    let len = current.len();
    metadata.exponential_histogram_count += len;
    metadata.record_metric_timestamp("exponential_histogram", current.first_timestamp());
    let batch = current.finish(schema)?;
    batches.push(("exponential_histogram".to_string(), batch));
    Ok(())
}

fn flush_summary_builder(
    builder: &mut SummaryBuilder,
    schema: Arc<arrow::datatypes::Schema>,
    metadata: &mut MetricsMetadata,
    batches: &mut Vec<(String, RecordBatch)>,
) -> Result<()> {
    if builder.len() == 0 {
        return Ok(());
    }

    let current = mem::replace(
        builder,
        SummaryBuilder::with_capacity(METRIC_MAX_ROWS_PER_BATCH),
    );
    let len = current.len();
    metadata.summary_count += len;
    metadata.record_metric_timestamp("summary", current.first_timestamp());
    let batch = current.finish(schema)?;
    batches.push(("summary".to_string(), batch));
    Ok(())
}

// Context structures for resource and scope information
struct ResourceContext {
    service_name: String,
    attributes_json: Cow<'static, str>,
}

struct ScopeContext {
    name: String,
    version: Option<String>,
}

fn extract_resource_context(resource_metrics: &ResourceMetrics) -> ResourceContext {
    let mut service_name = String::new();
    let mut attributes = Vec::new();

    if let Some(resource) = &resource_metrics.resource {
        for attr in &resource.attributes {
            let value_str = key_value_to_string(attr);

            // Extract service.name
            if attr.key == semconv::SERVICE_NAME {
                service_name = value_str.clone();
            }

            // Store all attributes
            attributes.push((attr.key.clone(), value_str));
        }
    }

    ResourceContext {
        service_name,
        attributes_json: string_pairs_to_json(&attributes),
    }
}

fn extract_scope_context(scope_metrics: &ScopeMetrics) -> ScopeContext {
    let mut name = String::new();
    let mut version = None;

    if let Some(scope) = &scope_metrics.scope {
        name = scope.name.clone();
        if !scope.version.is_empty() {
            version = Some(scope.version.clone());
        }
    }

    ScopeContext { name, version }
}

fn key_value_to_string(kv: &KeyValue) -> String {
    kv.value
        .as_ref()
        .and_then(any_value_string)
        .unwrap_or("")
        .to_string()
}

// Base columns builder for common fields across all metric types
struct BaseColumnsBuilder {
    timestamp_builder: TimestampMicrosecondBuilder,
    service_name_builder: StringBuilder,
    metric_name_builder: StringBuilder,
    metric_description_builder: StringBuilder,
    metric_unit_builder: StringBuilder,
    resource_attributes_builder: StringBuilder,
    scope_name_builder: StringBuilder,
    scope_version_builder: StringBuilder,
    attributes_builder: StringBuilder,
    count: usize,
    first_timestamp: Option<i64>,
}

impl BaseColumnsBuilder {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            timestamp_builder: TimestampMicrosecondBuilder::with_capacity(capacity)
                .with_timezone("UTC"),
            service_name_builder: StringBuilder::with_capacity(capacity, capacity * 32),
            metric_name_builder: StringBuilder::with_capacity(capacity, capacity * 32),
            metric_description_builder: StringBuilder::with_capacity(capacity, capacity * 64),
            metric_unit_builder: StringBuilder::with_capacity(capacity, capacity * 16),
            resource_attributes_builder: StringBuilder::with_capacity(capacity, capacity * 256),
            scope_name_builder: StringBuilder::with_capacity(capacity, capacity * 24),
            scope_version_builder: StringBuilder::with_capacity(capacity, capacity * 16),
            attributes_builder: StringBuilder::with_capacity(capacity, capacity * 128),
            count: 0,
            first_timestamp: None,
        }
    }

    fn add_common_fields(
        &mut self,
        metric: &Metric,
        timestamp_nanos: i64,
        attributes: &[KeyValue],
        resource_ctx: &ResourceContext,
        scope_ctx: &ScopeContext,
    ) -> Result<()> {
        // Timestamp
        self.timestamp_builder.append_value(timestamp_nanos);
        self.record_timestamp(timestamp_nanos);

        // Service name
        self.service_name_builder
            .append_value(&resource_ctx.service_name);

        // Metric metadata
        self.metric_name_builder.append_value(&metric.name);
        if metric.description.is_empty() {
            self.metric_description_builder.append_null();
        } else {
            self.metric_description_builder
                .append_value(&metric.description);
        }
        if metric.unit.is_empty() {
            self.metric_unit_builder.append_null();
        } else {
            self.metric_unit_builder.append_value(&metric.unit);
        }

        // Resource attributes - JSON-encoded string for S3 Tables compatibility
        self.resource_attributes_builder
            .append_value(resource_ctx.attributes_json.as_ref());

        // Scope information
        if scope_ctx.name.is_empty() {
            self.scope_name_builder.append_null();
        } else {
            self.scope_name_builder.append_value(&scope_ctx.name);
        }
        if let Some(version) = &scope_ctx.version {
            self.scope_version_builder.append_value(version);
        } else {
            self.scope_version_builder.append_null();
        }

        // Data point attributes - JSON-encoded string for S3 Tables compatibility
        let attributes_json = keyvalue_to_json(attributes);
        self.attributes_builder
            .append_value(attributes_json.as_ref());

        self.count += 1;
        Ok(())
    }

    fn len(&self) -> usize {
        self.count
    }

    fn first_timestamp(&self) -> Option<i64> {
        self.first_timestamp
    }

    fn record_timestamp(&mut self, timestamp: i64) {
        match self.first_timestamp {
            Some(existing) if existing <= timestamp => {}
            _ => self.first_timestamp = Some(timestamp),
        }
    }
}

// Gauge builder
struct GaugeBuilder {
    base: BaseColumnsBuilder,
    value_builder: Float64Builder,
}

impl GaugeBuilder {
    fn new() -> Self {
        Self::with_capacity(METRIC_DEFAULT_BUILDER_CAPACITY)
    }

    fn with_capacity(capacity: usize) -> Self {
        Self {
            base: BaseColumnsBuilder::with_capacity(capacity),
            value_builder: Float64Builder::with_capacity(capacity),
        }
    }

    fn add_data_point(
        &mut self,
        metric: &Metric,
        point: &NumberDataPoint,
        resource_ctx: &ResourceContext,
        scope_ctx: &ScopeContext,
    ) -> Result<()> {
        let timestamp = nanos_to_micros(point.time_unix_nano);
        self.base.add_common_fields(
            metric,
            timestamp,
            &point.attributes,
            resource_ctx,
            scope_ctx,
        )?;

        // Value
        let value = extract_number_value(point)?;
        self.value_builder.append_value(value);

        Ok(())
    }

    fn len(&self) -> usize {
        self.base.len()
    }

    fn first_timestamp(&self) -> Option<i64> {
        self.base.first_timestamp()
    }

    fn finish(mut self, schema: Arc<arrow::datatypes::Schema>) -> Result<RecordBatch> {
        let batch = RecordBatch::try_new(
            schema,
            vec![
                // Common fields (IDs 1, 4, 7, 9, 10)
                Arc::new(self.base.timestamp_builder.finish()),
                Arc::new(self.base.service_name_builder.finish()),
                Arc::new(self.base.resource_attributes_builder.finish()),
                Arc::new(self.base.scope_name_builder.finish()),
                Arc::new(self.base.scope_version_builder.finish()),
                // Metrics base fields (IDs 101-104)
                Arc::new(self.base.metric_name_builder.finish()),
                Arc::new(self.base.metric_description_builder.finish()),
                Arc::new(self.base.metric_unit_builder.finish()),
                Arc::new(self.base.attributes_builder.finish()),
                // Gauge-specific fields (IDs 110+)
                Arc::new(self.value_builder.finish()),
            ],
        )?;
        Ok(batch)
    }
}

// Sum builder
struct SumBuilder {
    base: BaseColumnsBuilder,
    value_builder: Float64Builder,
    aggregation_temporality_builder: Int32Builder,
    is_monotonic_builder: BooleanBuilder,
}

impl SumBuilder {
    fn new() -> Self {
        Self::with_capacity(METRIC_DEFAULT_BUILDER_CAPACITY)
    }

    fn with_capacity(capacity: usize) -> Self {
        Self {
            base: BaseColumnsBuilder::with_capacity(capacity),
            value_builder: Float64Builder::with_capacity(capacity),
            aggregation_temporality_builder: Int32Builder::with_capacity(capacity),
            is_monotonic_builder: BooleanBuilder::with_capacity(capacity),
        }
    }

    fn add_data_point(
        &mut self,
        metric: &Metric,
        point: &NumberDataPoint,
        aggregation_temporality: i32,
        is_monotonic: bool,
        resource_ctx: &ResourceContext,
        scope_ctx: &ScopeContext,
    ) -> Result<()> {
        let timestamp = nanos_to_micros(point.time_unix_nano);
        self.base.add_common_fields(
            metric,
            timestamp,
            &point.attributes,
            resource_ctx,
            scope_ctx,
        )?;

        let value = extract_number_value(point)?;
        self.value_builder.append_value(value);
        self.aggregation_temporality_builder
            .append_value(aggregation_temporality);
        self.is_monotonic_builder.append_value(is_monotonic);

        Ok(())
    }

    fn len(&self) -> usize {
        self.base.len()
    }

    fn first_timestamp(&self) -> Option<i64> {
        self.base.first_timestamp()
    }

    fn finish(mut self, schema: Arc<arrow::datatypes::Schema>) -> Result<RecordBatch> {
        let batch = RecordBatch::try_new(
            schema,
            vec![
                // Common fields (IDs 1, 4, 7, 9, 10)
                Arc::new(self.base.timestamp_builder.finish()),
                Arc::new(self.base.service_name_builder.finish()),
                Arc::new(self.base.resource_attributes_builder.finish()),
                Arc::new(self.base.scope_name_builder.finish()),
                Arc::new(self.base.scope_version_builder.finish()),
                // Metrics base fields (IDs 101-104)
                Arc::new(self.base.metric_name_builder.finish()),
                Arc::new(self.base.metric_description_builder.finish()),
                Arc::new(self.base.metric_unit_builder.finish()),
                Arc::new(self.base.attributes_builder.finish()),
                // Sum-specific fields (IDs 110+)
                Arc::new(self.value_builder.finish()),
                Arc::new(self.aggregation_temporality_builder.finish()),
                Arc::new(self.is_monotonic_builder.finish()),
            ],
        )?;
        Ok(batch)
    }
}

// Histogram builder
struct HistogramBuilder {
    base: BaseColumnsBuilder,
    count_builder: Int64Builder,
    sum_builder: Float64Builder,
    bucket_counts_builder: ListBuilder<Int64Builder>,
    explicit_bounds_builder: ListBuilder<Float64Builder>,
    min_builder: Float64Builder,
    max_builder: Float64Builder,
}

impl HistogramBuilder {
    fn new() -> Self {
        Self::with_capacity(METRIC_DEFAULT_BUILDER_CAPACITY)
    }

    fn with_capacity(capacity: usize) -> Self {
        let schema = otel_metrics_histogram_schema_arc();
        // Get bucket_counts field (index 11) and explicit_bounds field (index 12)
        let bucket_counts_field = if let DataType::List(field) = schema.field(11).data_type() {
            field.as_ref().clone()
        } else {
            panic!("Expected List type for bucket_counts");
        };
        let explicit_bounds_field = if let DataType::List(field) = schema.field(12).data_type() {
            field.as_ref().clone()
        } else {
            panic!("Expected List type for explicit_bounds");
        };

        Self {
            base: BaseColumnsBuilder::with_capacity(capacity),
            count_builder: Int64Builder::with_capacity(capacity),
            sum_builder: Float64Builder::with_capacity(capacity),
            bucket_counts_builder: ListBuilder::new(Int64Builder::with_capacity(capacity))
                .with_field(bucket_counts_field),
            explicit_bounds_builder: ListBuilder::new(Float64Builder::with_capacity(capacity))
                .with_field(explicit_bounds_field),
            min_builder: Float64Builder::with_capacity(capacity),
            max_builder: Float64Builder::with_capacity(capacity),
        }
    }

    fn add_data_point(
        &mut self,
        metric: &Metric,
        point: &otlp2parquet_proto::opentelemetry::proto::metrics::v1::HistogramDataPoint,
        resource_ctx: &ResourceContext,
        scope_ctx: &ScopeContext,
    ) -> Result<()> {
        let timestamp = nanos_to_micros(point.time_unix_nano);
        self.base.add_common_fields(
            metric,
            timestamp,
            &point.attributes,
            resource_ctx,
            scope_ctx,
        )?;

        self.count_builder.append_value(point.count as i64);
        self.sum_builder.append_value(point.sum.unwrap_or(0.0));

        // Bucket counts
        for &count in &point.bucket_counts {
            self.bucket_counts_builder
                .values()
                .append_value(count as i64);
        }
        self.bucket_counts_builder.append(true);

        // Explicit bounds
        for &bound in &point.explicit_bounds {
            self.explicit_bounds_builder.values().append_value(bound);
        }
        self.explicit_bounds_builder.append(true);

        // Min/Max (optional)
        if let Some(min) = point.min {
            self.min_builder.append_value(min);
        } else {
            self.min_builder.append_null();
        }
        if let Some(max) = point.max {
            self.max_builder.append_value(max);
        } else {
            self.max_builder.append_null();
        }

        Ok(())
    }

    fn len(&self) -> usize {
        self.base.len()
    }

    fn first_timestamp(&self) -> Option<i64> {
        self.base.first_timestamp()
    }

    fn finish(mut self, schema: Arc<arrow::datatypes::Schema>) -> Result<RecordBatch> {
        // Get list element fields from schema (with field_id metadata)
        let bucket_counts_field = if let DataType::List(field) = schema.field(11).data_type() {
            field.as_ref().clone()
        } else {
            panic!("Expected List type for bucket_counts");
        };
        let explicit_bounds_field = if let DataType::List(field) = schema.field(12).data_type() {
            field.as_ref().clone()
        } else {
            panic!("Expected List type for explicit_bounds");
        };

        // Convert list arrays to use schema fields (with field_id metadata)
        let bucket_counts =
            list_array_with_field(self.bucket_counts_builder.finish(), bucket_counts_field);
        let explicit_bounds =
            list_array_with_field(self.explicit_bounds_builder.finish(), explicit_bounds_field);

        let batch = RecordBatch::try_new(
            schema,
            vec![
                // Common fields (IDs 1, 4, 7, 9, 10)
                Arc::new(self.base.timestamp_builder.finish()),
                Arc::new(self.base.service_name_builder.finish()),
                Arc::new(self.base.resource_attributes_builder.finish()),
                Arc::new(self.base.scope_name_builder.finish()),
                Arc::new(self.base.scope_version_builder.finish()),
                // Metrics base fields (IDs 101-104)
                Arc::new(self.base.metric_name_builder.finish()),
                Arc::new(self.base.metric_description_builder.finish()),
                Arc::new(self.base.metric_unit_builder.finish()),
                Arc::new(self.base.attributes_builder.finish()),
                // Histogram-specific fields (IDs 110+)
                Arc::new(self.count_builder.finish()),
                Arc::new(self.sum_builder.finish()),
                Arc::new(bucket_counts),
                Arc::new(explicit_bounds),
                Arc::new(self.min_builder.finish()),
                Arc::new(self.max_builder.finish()),
            ],
        )?;
        Ok(batch)
    }
}

// Exponential Histogram builder
struct ExponentialHistogramBuilder {
    base: BaseColumnsBuilder,
    count_builder: Int64Builder,
    sum_builder: Float64Builder,
    scale_builder: Int32Builder,
    zero_count_builder: Int64Builder,
    positive_offset_builder: Int32Builder,
    positive_bucket_counts_builder: ListBuilder<Int64Builder>,
    negative_offset_builder: Int32Builder,
    negative_bucket_counts_builder: ListBuilder<Int64Builder>,
    min_builder: Float64Builder,
    max_builder: Float64Builder,
}

impl ExponentialHistogramBuilder {
    fn new() -> Self {
        Self::with_capacity(METRIC_DEFAULT_BUILDER_CAPACITY)
    }

    fn with_capacity(capacity: usize) -> Self {
        let schema = otel_metrics_exponential_histogram_schema_arc();
        // Get positive_bucket_counts field (index 14) and negative_bucket_counts field (index 16)
        let positive_bucket_counts_field =
            if let DataType::List(field) = schema.field(14).data_type() {
                field.as_ref().clone()
            } else {
                panic!("Expected List type for positive_bucket_counts");
            };
        let negative_bucket_counts_field =
            if let DataType::List(field) = schema.field(16).data_type() {
                field.as_ref().clone()
            } else {
                panic!("Expected List type for negative_bucket_counts");
            };

        Self {
            base: BaseColumnsBuilder::with_capacity(capacity),
            count_builder: Int64Builder::with_capacity(capacity),
            sum_builder: Float64Builder::with_capacity(capacity),
            scale_builder: Int32Builder::with_capacity(capacity),
            zero_count_builder: Int64Builder::with_capacity(capacity),
            positive_offset_builder: Int32Builder::with_capacity(capacity),
            positive_bucket_counts_builder: ListBuilder::new(Int64Builder::with_capacity(capacity))
                .with_field(positive_bucket_counts_field),
            negative_offset_builder: Int32Builder::with_capacity(capacity),
            negative_bucket_counts_builder: ListBuilder::new(Int64Builder::with_capacity(capacity))
                .with_field(negative_bucket_counts_field),
            min_builder: Float64Builder::with_capacity(capacity),
            max_builder: Float64Builder::with_capacity(capacity),
        }
    }

    fn add_data_point(
        &mut self,
        metric: &Metric,
        point: &otlp2parquet_proto::opentelemetry::proto::metrics::v1::ExponentialHistogramDataPoint,
        resource_ctx: &ResourceContext,
        scope_ctx: &ScopeContext,
    ) -> Result<()> {
        let timestamp = nanos_to_micros(point.time_unix_nano);
        self.base.add_common_fields(
            metric,
            timestamp,
            &point.attributes,
            resource_ctx,
            scope_ctx,
        )?;

        self.count_builder.append_value(point.count as i64);
        self.sum_builder.append_value(point.sum.unwrap_or(0.0));
        self.scale_builder.append_value(point.scale);
        self.zero_count_builder
            .append_value(point.zero_count as i64);

        // Positive buckets
        if let Some(positive) = &point.positive {
            self.positive_offset_builder.append_value(positive.offset);
            for &count in &positive.bucket_counts {
                self.positive_bucket_counts_builder
                    .values()
                    .append_value(count as i64);
            }
        } else {
            self.positive_offset_builder.append_value(0);
        }
        self.positive_bucket_counts_builder.append(true);

        // Negative buckets
        if let Some(negative) = &point.negative {
            self.negative_offset_builder.append_value(negative.offset);
            for &count in &negative.bucket_counts {
                self.negative_bucket_counts_builder
                    .values()
                    .append_value(count as i64);
            }
        } else {
            self.negative_offset_builder.append_value(0);
        }
        self.negative_bucket_counts_builder.append(true);

        // Min/Max (optional)
        if let Some(min) = point.min {
            self.min_builder.append_value(min);
        } else {
            self.min_builder.append_null();
        }
        if let Some(max) = point.max {
            self.max_builder.append_value(max);
        } else {
            self.max_builder.append_null();
        }

        Ok(())
    }

    fn len(&self) -> usize {
        self.base.len()
    }

    fn first_timestamp(&self) -> Option<i64> {
        self.base.first_timestamp()
    }

    fn finish(mut self, schema: Arc<arrow::datatypes::Schema>) -> Result<RecordBatch> {
        // Get list element fields from schema (with field_id metadata)
        let positive_bucket_counts_field =
            if let DataType::List(field) = schema.field(14).data_type() {
                field.as_ref().clone()
            } else {
                panic!("Expected List type for positive_bucket_counts");
            };
        let negative_bucket_counts_field =
            if let DataType::List(field) = schema.field(16).data_type() {
                field.as_ref().clone()
            } else {
                panic!("Expected List type for negative_bucket_counts");
            };

        // Convert list arrays to use schema fields (with field_id metadata)
        let positive_bucket_counts = list_array_with_field(
            self.positive_bucket_counts_builder.finish(),
            positive_bucket_counts_field,
        );
        let negative_bucket_counts = list_array_with_field(
            self.negative_bucket_counts_builder.finish(),
            negative_bucket_counts_field,
        );

        let batch = RecordBatch::try_new(
            schema,
            vec![
                // Common fields (IDs 1, 4, 7, 9, 10)
                Arc::new(self.base.timestamp_builder.finish()),
                Arc::new(self.base.service_name_builder.finish()),
                Arc::new(self.base.resource_attributes_builder.finish()),
                Arc::new(self.base.scope_name_builder.finish()),
                Arc::new(self.base.scope_version_builder.finish()),
                // Metrics base fields (IDs 101-104)
                Arc::new(self.base.metric_name_builder.finish()),
                Arc::new(self.base.metric_description_builder.finish()),
                Arc::new(self.base.metric_unit_builder.finish()),
                Arc::new(self.base.attributes_builder.finish()),
                // ExponentialHistogram-specific fields (IDs 110+)
                Arc::new(self.count_builder.finish()),
                Arc::new(self.sum_builder.finish()),
                Arc::new(self.scale_builder.finish()),
                Arc::new(self.zero_count_builder.finish()),
                Arc::new(self.positive_offset_builder.finish()),
                Arc::new(positive_bucket_counts),
                Arc::new(self.negative_offset_builder.finish()),
                Arc::new(negative_bucket_counts),
                Arc::new(self.min_builder.finish()),
                Arc::new(self.max_builder.finish()),
            ],
        )?;
        Ok(batch)
    }
}

// Summary builder
struct SummaryBuilder {
    base: BaseColumnsBuilder,
    count_builder: Int64Builder,
    sum_builder: Float64Builder,
    quantile_values_builder: ListBuilder<Float64Builder>,
    quantile_quantiles_builder: ListBuilder<Float64Builder>,
}

impl SummaryBuilder {
    fn new() -> Self {
        Self::with_capacity(METRIC_DEFAULT_BUILDER_CAPACITY)
    }

    fn with_capacity(capacity: usize) -> Self {
        let schema = otel_metrics_summary_schema_arc();
        // Get quantile_values field (index 11) and quantile_quantiles field (index 12)
        let quantile_values_field = if let DataType::List(field) = schema.field(11).data_type() {
            field.as_ref().clone()
        } else {
            panic!("Expected List type for quantile_values");
        };
        let quantile_quantiles_field = if let DataType::List(field) = schema.field(12).data_type() {
            field.as_ref().clone()
        } else {
            panic!("Expected List type for quantile_quantiles");
        };

        Self {
            base: BaseColumnsBuilder::with_capacity(capacity),
            count_builder: Int64Builder::with_capacity(capacity),
            sum_builder: Float64Builder::with_capacity(capacity),
            quantile_values_builder: ListBuilder::new(Float64Builder::with_capacity(capacity))
                .with_field(quantile_values_field),
            quantile_quantiles_builder: ListBuilder::new(Float64Builder::with_capacity(capacity))
                .with_field(quantile_quantiles_field),
        }
    }

    fn add_data_point(
        &mut self,
        metric: &Metric,
        point: &otlp2parquet_proto::opentelemetry::proto::metrics::v1::SummaryDataPoint,
        resource_ctx: &ResourceContext,
        scope_ctx: &ScopeContext,
    ) -> Result<()> {
        let timestamp = nanos_to_micros(point.time_unix_nano);
        self.base.add_common_fields(
            metric,
            timestamp,
            &point.attributes,
            resource_ctx,
            scope_ctx,
        )?;

        self.count_builder.append_value(point.count as i64);
        self.sum_builder.append_value(point.sum);

        // Quantile values and quantiles
        for quantile_value in &point.quantile_values {
            self.quantile_values_builder
                .values()
                .append_value(quantile_value.value);
            self.quantile_quantiles_builder
                .values()
                .append_value(quantile_value.quantile);
        }
        self.quantile_values_builder.append(true);
        self.quantile_quantiles_builder.append(true);

        Ok(())
    }

    fn len(&self) -> usize {
        self.base.len()
    }

    fn first_timestamp(&self) -> Option<i64> {
        self.base.first_timestamp()
    }

    fn finish(mut self, schema: Arc<arrow::datatypes::Schema>) -> Result<RecordBatch> {
        // Get list element fields from schema (with field_id metadata)
        let quantile_values_field = if let DataType::List(field) = schema.field(11).data_type() {
            field.as_ref().clone()
        } else {
            panic!("Expected List type for quantile_values");
        };
        let quantile_quantiles_field = if let DataType::List(field) = schema.field(12).data_type() {
            field.as_ref().clone()
        } else {
            panic!("Expected List type for quantile_quantiles");
        };

        // Convert list arrays to use schema fields (with field_id metadata)
        let quantile_values =
            list_array_with_field(self.quantile_values_builder.finish(), quantile_values_field);
        let quantile_quantiles = list_array_with_field(
            self.quantile_quantiles_builder.finish(),
            quantile_quantiles_field,
        );

        let batch = RecordBatch::try_new(
            schema,
            vec![
                // Common fields (IDs 1, 4, 7, 9, 10)
                Arc::new(self.base.timestamp_builder.finish()),
                Arc::new(self.base.service_name_builder.finish()),
                Arc::new(self.base.resource_attributes_builder.finish()),
                Arc::new(self.base.scope_name_builder.finish()),
                Arc::new(self.base.scope_version_builder.finish()),
                // Metrics base fields (IDs 101-104)
                Arc::new(self.base.metric_name_builder.finish()),
                Arc::new(self.base.metric_description_builder.finish()),
                Arc::new(self.base.metric_unit_builder.finish()),
                Arc::new(self.base.attributes_builder.finish()),
                // Summary-specific fields (IDs 110+)
                Arc::new(self.count_builder.finish()),
                Arc::new(self.sum_builder.finish()),
                Arc::new(quantile_values),
                Arc::new(quantile_quantiles),
            ],
        )?;
        Ok(batch)
    }
}

// Helper functions

/// Convert OTLP nanosecond timestamps to microseconds for Parquet compatibility
#[inline]
fn nanos_to_micros(ns: u64) -> i64 {
    ((ns / 1_000).min(i64::MAX as u64)) as i64
}

fn extract_number_value(point: &NumberDataPoint) -> Result<f64> {
    match &point.value {
        Some(Value::AsDouble(v)) => Ok(*v),
        Some(Value::AsInt(v)) => Ok(*v as f64),
        None => anyhow::bail!("NumberDataPoint has no value"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::otlp::common::InputFormat;
    use crate::otlp::metrics::parse_otlp_request;
    use arrow::array::{ListArray, StringArray, TimestampMicrosecondArray};
    use arrow::record_batch::RecordBatch;

    #[test]
    fn test_converter_creation() {
        let converter = ArrowConverter::new();
        assert!(converter.schema_for_type("gauge").is_some());
        assert!(converter.schema_for_type("sum").is_some());
        assert!(converter.schema_for_type("histogram").is_some());
        assert!(converter.schema_for_type("exponential_histogram").is_some());
        assert!(converter.schema_for_type("summary").is_some());
        assert!(converter.schema_for_type("invalid").is_none());
    }

    #[test]
    fn test_empty_request() {
        let converter = ArrowConverter::new();
        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![],
        };

        let result = converter.convert(request);
        assert!(result.is_ok());

        let (batches, metadata) = result.unwrap();
        assert_eq!(batches.len(), 0);
        assert_eq!(metadata.resource_metrics_count, 0);
    }

    #[test]
    fn test_nanos_to_micros() {
        assert_eq!(nanos_to_micros(1_000_000), 1_000); // 1ms in nanos -> 1ms in micros
        assert_eq!(nanos_to_micros(1_000), 1); // 1us in nanos -> 1us in micros
        assert_eq!(nanos_to_micros(i64::MAX as u64), i64::MAX / 1_000);
        assert_eq!(nanos_to_micros(u64::MAX), (u64::MAX / 1_000) as i64);
    }

    #[test]
    fn test_gauge_conversion() {
        use otlp2parquet_proto::opentelemetry::proto::metrics::v1::{
            metric::Data, number_data_point::Value, Gauge, Metric, NumberDataPoint,
            ResourceMetrics, ScopeMetrics,
        };
        use otlp2parquet_proto::opentelemetry::proto::{
            common::v1::KeyValue, resource::v1::Resource,
        };

        let converter = ArrowConverter::new();

        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".to_string(),
                        value: Some(otlp2parquet_proto::opentelemetry::proto::common::v1::AnyValue {
                            value: Some(otlp2parquet_proto::opentelemetry::proto::common::v1::any_value::Value::StringValue("test-service".to_string())),
                        }),
                    }],
                    ..Default::default()
                }),
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![Metric {
                        name: "cpu.usage".to_string(),
                        description: "CPU usage percentage".to_string(),
                        unit: "%".to_string(),
                        data: Some(Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                attributes: vec![],
                                time_unix_nano: 1_705_327_800_000_000_000,
                                value: Some(Value::AsDouble(42.5)),
                                start_time_unix_nano: 0,
                                flags: 0,
                                exemplars: vec![],
                            }],
                        })),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let result = converter.convert(request);
        assert!(result.is_ok());

        let (batches, metadata) = result.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].0, "gauge");
        assert_eq!(metadata.gauge_count, 1);
        assert_eq!(metadata.sum_count, 0);

        let batch = &batches[0].1;
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 10); // 9 base + 1 value
    }

    #[test]
    fn test_sum_conversion() {
        use otlp2parquet_proto::opentelemetry::proto::metrics::v1::{
            metric::Data, number_data_point::Value, AggregationTemporality, Metric,
            NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum,
        };
        use otlp2parquet_proto::opentelemetry::proto::resource::v1::Resource;

        let converter = ArrowConverter::new();

        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource::default()),
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![Metric {
                        name: "requests.total".to_string(),
                        description: String::new(),
                        unit: "1".to_string(),
                        data: Some(Data::Sum(Sum {
                            data_points: vec![NumberDataPoint {
                                attributes: vec![],
                                time_unix_nano: 1_705_327_800_000_000_000,
                                value: Some(Value::AsInt(1000)),
                                start_time_unix_nano: 0,
                                flags: 0,
                                exemplars: vec![],
                            }],
                            aggregation_temporality: AggregationTemporality::Cumulative as i32,
                            is_monotonic: true,
                        })),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let result = converter.convert(request);
        assert!(result.is_ok());

        let (batches, metadata) = result.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].0, "sum");
        assert_eq!(metadata.sum_count, 1);

        let batch = &batches[0].1;
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 12); // 9 base + 3 sum fields
    }

    #[test]
    fn test_histogram_conversion() {
        use otlp2parquet_proto::opentelemetry::proto::metrics::v1::{
            metric::Data, Histogram, HistogramDataPoint, Metric, ResourceMetrics, ScopeMetrics,
        };
        use otlp2parquet_proto::opentelemetry::proto::resource::v1::Resource;

        let converter = ArrowConverter::new();

        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource::default()),
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![Metric {
                        name: "request.duration".to_string(),
                        description: String::new(),
                        unit: "ms".to_string(),
                        data: Some(Data::Histogram(Histogram {
                            data_points: vec![HistogramDataPoint {
                                attributes: vec![],
                                time_unix_nano: 1_705_327_800_000_000_000,
                                count: 100,
                                sum: Some(5000.0),
                                bucket_counts: vec![10, 30, 40, 15, 5],
                                explicit_bounds: vec![10.0, 50.0, 100.0, 500.0],
                                min: Some(5.0),
                                max: Some(1000.0),
                                start_time_unix_nano: 0,
                                flags: 0,
                                exemplars: vec![],
                            }],
                            aggregation_temporality: 0,
                        })),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let result = converter.convert(request);
        if let Err(e) = &result {
            eprintln!("Conversion error: {:?}", e);
            panic!("Failed to convert histogram: {:?}", e);
        }
        let (batches, metadata) = result.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].0, "histogram");
        assert_eq!(metadata.histogram_count, 1);

        let batch = &batches[0].1;
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 15); // 9 base + 6 histogram fields
    }

    #[test]
    fn test_multiple_metric_types() {
        use otlp2parquet_proto::opentelemetry::proto::metrics::v1::{
            metric::Data, number_data_point::Value, Gauge, Histogram, HistogramDataPoint, Metric,
            NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum,
        };
        use otlp2parquet_proto::opentelemetry::proto::resource::v1::Resource;

        let converter = ArrowConverter::new();

        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource::default()),
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![
                        Metric {
                            name: "cpu.usage".to_string(),
                            description: String::new(),
                            unit: "%".to_string(),
                            data: Some(Data::Gauge(Gauge {
                                data_points: vec![NumberDataPoint {
                                    attributes: vec![],
                                    time_unix_nano: 1_705_327_800_000_000_000,
                                    value: Some(Value::AsDouble(75.0)),
                                    start_time_unix_nano: 0,
                                    flags: 0,
                                    exemplars: vec![],
                                }],
                            })),
                        },
                        Metric {
                            name: "requests.total".to_string(),
                            description: String::new(),
                            unit: "1".to_string(),
                            data: Some(Data::Sum(Sum {
                                data_points: vec![NumberDataPoint {
                                    attributes: vec![],
                                    time_unix_nano: 1_705_327_800_000_000_000,
                                    value: Some(Value::AsInt(500)),
                                    start_time_unix_nano: 0,
                                    flags: 0,
                                    exemplars: vec![],
                                }],
                                aggregation_temporality: 2, // Cumulative
                                is_monotonic: true,
                            })),
                        },
                        Metric {
                            name: "latency".to_string(),
                            description: String::new(),
                            unit: "ms".to_string(),
                            data: Some(Data::Histogram(Histogram {
                                data_points: vec![HistogramDataPoint {
                                    attributes: vec![],
                                    time_unix_nano: 1_705_327_800_000_000_000,
                                    count: 50,
                                    sum: Some(2500.0),
                                    bucket_counts: vec![5, 15, 20, 10],
                                    explicit_bounds: vec![10.0, 50.0, 100.0],
                                    min: None,
                                    max: None,
                                    start_time_unix_nano: 0,
                                    flags: 0,
                                    exemplars: vec![],
                                }],
                                aggregation_temporality: 0,
                            })),
                        },
                    ],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let (batches, metadata) = converter
            .convert(request)
            .expect("Failed to convert multiple metrics");
        assert_eq!(batches.len(), 3); // gauge, sum, histogram
        assert_eq!(metadata.gauge_count, 1);
        assert_eq!(metadata.sum_count, 1);
        assert_eq!(metadata.histogram_count, 1);
        assert_eq!(metadata.exponential_histogram_count, 0);
        assert_eq!(metadata.summary_count, 0);

        // Verify batch types
        let batch_types: Vec<&str> = batches.iter().map(|(t, _)| t.as_str()).collect();
        assert!(batch_types.contains(&"gauge"));
        assert!(batch_types.contains(&"sum"));
        assert!(batch_types.contains(&"histogram"));
    }

    fn convert_fixture(bytes: &[u8]) -> (Vec<(String, RecordBatch)>, MetricsMetadata) {
        let request = parse_otlp_request(bytes, InputFormat::Protobuf).unwrap();
        ArrowConverter::new().convert(request).unwrap()
    }

    fn find_batch<'a>(batches: &'a [(String, RecordBatch)], ty: &str) -> &'a RecordBatch {
        batches
            .iter()
            .find(|(name, _)| name == ty)
            .map(|(_, batch)| batch)
            .expect("expected metric batch")
    }

    #[test]
    fn converts_gauge_protobuf_fixture() {
        let bytes = include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../testdata/metrics_gauge.pb"
        ));
        let (batches, metadata) = convert_fixture(bytes);

        assert_eq!(metadata.resource_metrics_count, 1);
        assert_eq!(metadata.scope_metrics_count, 1);
        assert_eq!(metadata.gauge_count, 3);
        assert_eq!(metadata.sum_count, 0);
        assert_eq!(metadata.service_name(), "demo-service");

        let batch = find_batch(&batches, "gauge");
        assert_eq!(batch.num_rows(), 3);
        let service_names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..service_names.len() {
            assert_eq!(service_names.value(row), "demo-service");
        }
        let timestamps = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(
            metadata.first_timestamp_for("gauge"),
            Some(timestamps.value(0))
        );
    }

    #[test]
    fn converts_sum_protobuf_fixture() {
        let bytes = include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../testdata/metrics_sum.pb"
        ));
        let (batches, metadata) = convert_fixture(bytes);

        assert_eq!(metadata.resource_metrics_count, 1);
        assert_eq!(metadata.scope_metrics_count, 1);
        assert_eq!(metadata.sum_count, 4);
        assert_eq!(metadata.gauge_count, 0);
        assert_eq!(metadata.service_name(), "api-gateway");

        let batch = find_batch(&batches, "sum");
        assert_eq!(batch.num_rows(), 4);
        let service_names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..service_names.len() {
            assert_eq!(service_names.value(row), "api-gateway");
        }
        let timestamps = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(
            metadata.first_timestamp_for("sum"),
            Some(timestamps.value(0))
        );
    }

    #[test]
    fn converts_histogram_protobuf_fixture() {
        let bytes = include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../testdata/metrics_histogram.pb"
        ));
        let (batches, metadata) = convert_fixture(bytes);

        assert_eq!(metadata.histogram_count, 3);
        assert_eq!(metadata.resource_metrics_count, 1);
        assert_eq!(metadata.service_name(), "api-gateway");

        let batch = find_batch(&batches, "histogram");
        assert_eq!(batch.num_rows(), 3);
        let service_names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..service_names.len() {
            assert_eq!(service_names.value(row), "api-gateway");
        }
    }

    #[test]
    fn converts_exponential_histogram_protobuf_fixture() {
        let bytes = include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../testdata/metrics_exponential_histogram.pb"
        ));
        let (batches, metadata) = convert_fixture(bytes);

        assert_eq!(metadata.exponential_histogram_count, 2);
        assert_eq!(metadata.resource_metrics_count, 1);
        assert_eq!(metadata.service_name(), "payment-service");

        let batch = find_batch(&batches, "exponential_histogram");
        assert_eq!(batch.num_rows(), 2);
        let service_names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..service_names.len() {
            assert_eq!(service_names.value(row), "payment-service");
        }
    }

    #[test]
    fn converts_summary_protobuf_fixture() {
        let bytes = include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../testdata/metrics_summary.pb"
        ));
        let (batches, metadata) = convert_fixture(bytes);

        assert_eq!(metadata.summary_count, 2);
        assert_eq!(metadata.resource_metrics_count, 1);
        assert_eq!(metadata.service_name(), "analytics-service");

        let batch = find_batch(&batches, "summary");
        assert_eq!(batch.num_rows(), 2);
        let service_names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..service_names.len() {
            assert_eq!(service_names.value(row), "analytics-service");
        }

        let quantiles = batch
            .column(11)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(quantiles.value_length(0), 6);
        assert_eq!(quantiles.value_length(1), 6);
    }
}

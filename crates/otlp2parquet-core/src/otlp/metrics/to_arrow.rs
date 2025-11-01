// OTLP Metrics to Arrow conversion
//
// This module handles converting OTLP metrics data to Arrow RecordBatches
// with separate schemas for each metric type (gauge, sum, histogram, etc.)

use anyhow::{Context, Result};
use arrow::array::{
    BooleanBuilder, Float64Builder, Int32Builder, ListBuilder, MapBuilder, RecordBatch,
    StringBuilder, TimestampNanosecondBuilder, UInt64Builder,
};
use otlp2parquet_proto::opentelemetry::proto::{
    collector::metrics::v1::ExportMetricsServiceRequest,
    common::v1::KeyValue,
    metrics::v1::{
        metric::Data, number_data_point::Value, Metric, NumberDataPoint, ResourceMetrics,
        ScopeMetrics,
    },
};
use std::collections::HashMap;
use std::sync::Arc;

use crate::otlp::common::{
    any_value_builder::any_value_string, builder_helpers::map_field_names,
    field_names::semconv,
};
use crate::schema::metrics::*;

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

        // Process all resource metrics
        for resource_metrics in &request.resource_metrics {
            let resource_ctx = extract_resource_context(resource_metrics);

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
                    )?;
                }
            }
        }

        // Build record batches for each metric type
        let mut batches = Vec::new();

        if gauge_builder.len() > 0 {
            metadata.gauge_count = gauge_builder.len();
            batches.push((
                "gauge".to_string(),
                gauge_builder.finish(Arc::clone(&self.schema_gauge))?,
            ));
        }

        if sum_builder.len() > 0 {
            metadata.sum_count = sum_builder.len();
            batches.push((
                "sum".to_string(),
                sum_builder.finish(Arc::clone(&self.schema_sum))?,
            ));
        }

        if histogram_builder.len() > 0 {
            metadata.histogram_count = histogram_builder.len();
            batches.push((
                "histogram".to_string(),
                histogram_builder.finish(Arc::clone(&self.schema_histogram))?,
            ));
        }

        if exp_histogram_builder.len() > 0 {
            metadata.exponential_histogram_count = exp_histogram_builder.len();
            batches.push((
                "exponential_histogram".to_string(),
                exp_histogram_builder.finish(Arc::clone(&self.schema_exponential_histogram))?,
            ));
        }

        if summary_builder.len() > 0 {
            metadata.summary_count = summary_builder.len();
            batches.push((
                "summary".to_string(),
                summary_builder.finish(Arc::clone(&self.schema_summary))?,
            ));
        }

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
    ) -> Result<()> {
        let data = metric.data.as_ref().context("Metric has no data")?;

        match data {
            Data::Gauge(gauge) => {
                for point in &gauge.data_points {
                    gauge_builder.add_data_point(metric, point, resource_ctx, scope_ctx)?;
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
                }
            }
            Data::Histogram(histogram) => {
                for point in &histogram.data_points {
                    histogram_builder.add_data_point(metric, point, resource_ctx, scope_ctx)?;
                }
            }
            Data::ExponentialHistogram(exp_histogram) => {
                for point in &exp_histogram.data_points {
                    exp_histogram_builder.add_data_point(
                        metric,
                        point,
                        resource_ctx,
                        scope_ctx,
                    )?;
                }
            }
            Data::Summary(summary) => {
                for point in &summary.data_points {
                    summary_builder.add_data_point(metric, point, resource_ctx, scope_ctx)?;
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

// Context structures for resource and scope information
struct ResourceContext {
    service_name: String,
    attributes: HashMap<String, String>,
}

struct ScopeContext {
    name: String,
    version: Option<String>,
}

fn extract_resource_context(resource_metrics: &ResourceMetrics) -> ResourceContext {
    let mut service_name = String::new();
    let mut attributes = HashMap::new();

    if let Some(resource) = &resource_metrics.resource {
        for attr in &resource.attributes {
            let value_str = key_value_to_string(attr);

            // Extract service.name
            if attr.key == semconv::SERVICE_NAME {
                service_name = value_str.clone();
            }

            // Store all attributes
            attributes.insert(attr.key.clone(), value_str);
        }
    }

    ResourceContext {
        service_name,
        attributes,
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
    timestamp_builder: TimestampNanosecondBuilder,
    service_name_builder: StringBuilder,
    metric_name_builder: StringBuilder,
    metric_description_builder: StringBuilder,
    metric_unit_builder: StringBuilder,
    resource_attributes_builder: MapBuilder<StringBuilder, StringBuilder>,
    scope_name_builder: StringBuilder,
    scope_version_builder: StringBuilder,
    attributes_builder: MapBuilder<StringBuilder, StringBuilder>,
    count: usize,
}

impl BaseColumnsBuilder {
    fn new() -> Self {
        Self {
            timestamp_builder: TimestampNanosecondBuilder::new().with_timezone("UTC"),
            service_name_builder: StringBuilder::new(),
            metric_name_builder: StringBuilder::new(),
            metric_description_builder: StringBuilder::new(),
            metric_unit_builder: StringBuilder::new(),
            resource_attributes_builder: MapBuilder::new(
                Some(map_field_names()),
                StringBuilder::new(),
                StringBuilder::new(),
            ),
            scope_name_builder: StringBuilder::new(),
            scope_version_builder: StringBuilder::new(),
            attributes_builder: MapBuilder::new(
                Some(map_field_names()),
                StringBuilder::new(),
                StringBuilder::new(),
            ),
            count: 0,
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

        // Resource attributes
        for (key, value) in &resource_ctx.attributes {
            self.resource_attributes_builder.keys().append_value(key);
            self.resource_attributes_builder.values().append_value(value);
        }
        self.resource_attributes_builder.append(true)?;

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

        // Data point attributes
        for attr in attributes {
            self.attributes_builder.keys().append_value(&attr.key);
            self.attributes_builder
                .values()
                .append_value(key_value_to_string(attr));
        }
        self.attributes_builder.append(true)?;

        self.count += 1;
        Ok(())
    }

    fn len(&self) -> usize {
        self.count
    }
}

// Gauge builder
struct GaugeBuilder {
    base: BaseColumnsBuilder,
    value_builder: Float64Builder,
}

impl GaugeBuilder {
    fn new() -> Self {
        Self {
            base: BaseColumnsBuilder::new(),
            value_builder: Float64Builder::new(),
        }
    }

    fn add_data_point(
        &mut self,
        metric: &Metric,
        point: &NumberDataPoint,
        resource_ctx: &ResourceContext,
        scope_ctx: &ScopeContext,
    ) -> Result<()> {
        let timestamp = clamp_nanos(point.time_unix_nano);
        self.base
            .add_common_fields(metric, timestamp, &point.attributes, resource_ctx, scope_ctx)?;

        // Value
        let value = extract_number_value(point)?;
        self.value_builder.append_value(value);

        Ok(())
    }

    fn len(&self) -> usize {
        self.base.len()
    }

    fn finish(mut self, schema: Arc<arrow::datatypes::Schema>) -> Result<RecordBatch> {
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(self.base.timestamp_builder.finish()),
                Arc::new(self.base.service_name_builder.finish()),
                Arc::new(self.base.metric_name_builder.finish()),
                Arc::new(self.base.metric_description_builder.finish()),
                Arc::new(self.base.metric_unit_builder.finish()),
                Arc::new(self.base.resource_attributes_builder.finish()),
                Arc::new(self.base.scope_name_builder.finish()),
                Arc::new(self.base.scope_version_builder.finish()),
                Arc::new(self.base.attributes_builder.finish()),
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
        Self {
            base: BaseColumnsBuilder::new(),
            value_builder: Float64Builder::new(),
            aggregation_temporality_builder: Int32Builder::new(),
            is_monotonic_builder: BooleanBuilder::new(),
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
        let timestamp = clamp_nanos(point.time_unix_nano);
        self.base
            .add_common_fields(metric, timestamp, &point.attributes, resource_ctx, scope_ctx)?;

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

    fn finish(mut self, schema: Arc<arrow::datatypes::Schema>) -> Result<RecordBatch> {
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(self.base.timestamp_builder.finish()),
                Arc::new(self.base.service_name_builder.finish()),
                Arc::new(self.base.metric_name_builder.finish()),
                Arc::new(self.base.metric_description_builder.finish()),
                Arc::new(self.base.metric_unit_builder.finish()),
                Arc::new(self.base.resource_attributes_builder.finish()),
                Arc::new(self.base.scope_name_builder.finish()),
                Arc::new(self.base.scope_version_builder.finish()),
                Arc::new(self.base.attributes_builder.finish()),
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
    count_builder: UInt64Builder,
    sum_builder: Float64Builder,
    bucket_counts_builder: ListBuilder<UInt64Builder>,
    explicit_bounds_builder: ListBuilder<Float64Builder>,
    min_builder: Float64Builder,
    max_builder: Float64Builder,
}

impl HistogramBuilder {
    fn new() -> Self {
        Self {
            base: BaseColumnsBuilder::new(),
            count_builder: UInt64Builder::new(),
            sum_builder: Float64Builder::new(),
            bucket_counts_builder: ListBuilder::new(UInt64Builder::new()),
            explicit_bounds_builder: ListBuilder::new(Float64Builder::new()),
            min_builder: Float64Builder::new(),
            max_builder: Float64Builder::new(),
        }
    }

    fn add_data_point(
        &mut self,
        metric: &Metric,
        point: &otlp2parquet_proto::opentelemetry::proto::metrics::v1::HistogramDataPoint,
        resource_ctx: &ResourceContext,
        scope_ctx: &ScopeContext,
    ) -> Result<()> {
        let timestamp = clamp_nanos(point.time_unix_nano);
        self.base
            .add_common_fields(metric, timestamp, &point.attributes, resource_ctx, scope_ctx)?;

        self.count_builder.append_value(point.count);
        self.sum_builder.append_value(point.sum.unwrap_or(0.0));

        // Bucket counts
        for &count in &point.bucket_counts {
            self.bucket_counts_builder.values().append_value(count);
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

    fn finish(mut self, schema: Arc<arrow::datatypes::Schema>) -> Result<RecordBatch> {
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(self.base.timestamp_builder.finish()),
                Arc::new(self.base.service_name_builder.finish()),
                Arc::new(self.base.metric_name_builder.finish()),
                Arc::new(self.base.metric_description_builder.finish()),
                Arc::new(self.base.metric_unit_builder.finish()),
                Arc::new(self.base.resource_attributes_builder.finish()),
                Arc::new(self.base.scope_name_builder.finish()),
                Arc::new(self.base.scope_version_builder.finish()),
                Arc::new(self.base.attributes_builder.finish()),
                Arc::new(self.count_builder.finish()),
                Arc::new(self.sum_builder.finish()),
                Arc::new(self.bucket_counts_builder.finish()),
                Arc::new(self.explicit_bounds_builder.finish()),
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
    count_builder: UInt64Builder,
    sum_builder: Float64Builder,
    scale_builder: Int32Builder,
    zero_count_builder: UInt64Builder,
    positive_offset_builder: Int32Builder,
    positive_bucket_counts_builder: ListBuilder<UInt64Builder>,
    negative_offset_builder: Int32Builder,
    negative_bucket_counts_builder: ListBuilder<UInt64Builder>,
    min_builder: Float64Builder,
    max_builder: Float64Builder,
}

impl ExponentialHistogramBuilder {
    fn new() -> Self {
        Self {
            base: BaseColumnsBuilder::new(),
            count_builder: UInt64Builder::new(),
            sum_builder: Float64Builder::new(),
            scale_builder: Int32Builder::new(),
            zero_count_builder: UInt64Builder::new(),
            positive_offset_builder: Int32Builder::new(),
            positive_bucket_counts_builder: ListBuilder::new(UInt64Builder::new()),
            negative_offset_builder: Int32Builder::new(),
            negative_bucket_counts_builder: ListBuilder::new(UInt64Builder::new()),
            min_builder: Float64Builder::new(),
            max_builder: Float64Builder::new(),
        }
    }

    fn add_data_point(
        &mut self,
        metric: &Metric,
        point: &otlp2parquet_proto::opentelemetry::proto::metrics::v1::ExponentialHistogramDataPoint,
        resource_ctx: &ResourceContext,
        scope_ctx: &ScopeContext,
    ) -> Result<()> {
        let timestamp = clamp_nanos(point.time_unix_nano);
        self.base
            .add_common_fields(metric, timestamp, &point.attributes, resource_ctx, scope_ctx)?;

        self.count_builder.append_value(point.count);
        self.sum_builder.append_value(point.sum.unwrap_or(0.0));
        self.scale_builder.append_value(point.scale);
        self.zero_count_builder.append_value(point.zero_count);

        // Positive buckets
        if let Some(positive) = &point.positive {
            self.positive_offset_builder.append_value(positive.offset);
            for &count in &positive.bucket_counts {
                self.positive_bucket_counts_builder
                    .values()
                    .append_value(count);
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
                    .append_value(count);
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

    fn finish(mut self, schema: Arc<arrow::datatypes::Schema>) -> Result<RecordBatch> {
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(self.base.timestamp_builder.finish()),
                Arc::new(self.base.service_name_builder.finish()),
                Arc::new(self.base.metric_name_builder.finish()),
                Arc::new(self.base.metric_description_builder.finish()),
                Arc::new(self.base.metric_unit_builder.finish()),
                Arc::new(self.base.resource_attributes_builder.finish()),
                Arc::new(self.base.scope_name_builder.finish()),
                Arc::new(self.base.scope_version_builder.finish()),
                Arc::new(self.base.attributes_builder.finish()),
                Arc::new(self.count_builder.finish()),
                Arc::new(self.sum_builder.finish()),
                Arc::new(self.scale_builder.finish()),
                Arc::new(self.zero_count_builder.finish()),
                Arc::new(self.positive_offset_builder.finish()),
                Arc::new(self.positive_bucket_counts_builder.finish()),
                Arc::new(self.negative_offset_builder.finish()),
                Arc::new(self.negative_bucket_counts_builder.finish()),
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
    count_builder: UInt64Builder,
    sum_builder: Float64Builder,
    quantile_values_builder: ListBuilder<Float64Builder>,
    quantile_quantiles_builder: ListBuilder<Float64Builder>,
}

impl SummaryBuilder {
    fn new() -> Self {
        Self {
            base: BaseColumnsBuilder::new(),
            count_builder: UInt64Builder::new(),
            sum_builder: Float64Builder::new(),
            quantile_values_builder: ListBuilder::new(Float64Builder::new()),
            quantile_quantiles_builder: ListBuilder::new(Float64Builder::new()),
        }
    }

    fn add_data_point(
        &mut self,
        metric: &Metric,
        point: &otlp2parquet_proto::opentelemetry::proto::metrics::v1::SummaryDataPoint,
        resource_ctx: &ResourceContext,
        scope_ctx: &ScopeContext,
    ) -> Result<()> {
        let timestamp = clamp_nanos(point.time_unix_nano);
        self.base
            .add_common_fields(metric, timestamp, &point.attributes, resource_ctx, scope_ctx)?;

        self.count_builder.append_value(point.count);
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

    fn finish(mut self, schema: Arc<arrow::datatypes::Schema>) -> Result<RecordBatch> {
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(self.base.timestamp_builder.finish()),
                Arc::new(self.base.service_name_builder.finish()),
                Arc::new(self.base.metric_name_builder.finish()),
                Arc::new(self.base.metric_description_builder.finish()),
                Arc::new(self.base.metric_unit_builder.finish()),
                Arc::new(self.base.resource_attributes_builder.finish()),
                Arc::new(self.base.scope_name_builder.finish()),
                Arc::new(self.base.scope_version_builder.finish()),
                Arc::new(self.base.attributes_builder.finish()),
                Arc::new(self.count_builder.finish()),
                Arc::new(self.sum_builder.finish()),
                Arc::new(self.quantile_values_builder.finish()),
                Arc::new(self.quantile_quantiles_builder.finish()),
            ],
        )?;
        Ok(batch)
    }
}

// Helper functions

#[inline]
fn clamp_nanos(ns: u64) -> i64 {
    (ns.min(i64::MAX as u64)) as i64
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
    fn test_clamp_nanos() {
        assert_eq!(clamp_nanos(1000), 1000);
        assert_eq!(clamp_nanos(i64::MAX as u64), i64::MAX);
        assert_eq!(clamp_nanos(u64::MAX), i64::MAX);
    }
}

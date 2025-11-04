#!/usr/bin/env -S uv run --quiet --script
# /// script
# dependencies = [
#   "opentelemetry-proto>=1.20.0",
#   "protobuf>=4.0.0",
# ]
# requires-python = ">=3.11"
# ///

"""
Deterministic OpenTelemetry Test Data Generator

Generates all testdata files using OpenTelemetry protobuf structures with a fixed seed
for reproducibility. Supports logs, metrics (all 5 types), and traces in JSON,
JSONL, and Protobuf formats.

Usage:
    ./scripts/generate_testdata.py                      # Generate all testdata
    ./scripts/generate_testdata.py --only logs          # Only logs
    ./scripts/generate_testdata.py --only metrics-gauge # Specific metric type
    ./scripts/generate_testdata.py --verbose            # Verbose output
    ./scripts/generate_testdata.py --size-mb 50         # Generate ~50MB files (uses _large suffix)
    ./scripts/generate_testdata.py --size-mb 100 --only logs  # 100MB logs only
"""

import argparse
import json
import random
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from google.protobuf import json_format
from opentelemetry.proto.common.v1.common_pb2 import AnyValue, KeyValue, InstrumentationScope
from opentelemetry.proto.resource.v1.resource_pb2 import Resource
from opentelemetry.proto.logs.v1.logs_pb2 import LogsData, ResourceLogs, ScopeLogs, LogRecord, SeverityNumber
from opentelemetry.proto.metrics.v1.metrics_pb2 import (
    MetricsData, ResourceMetrics, ScopeMetrics, Metric,
    Gauge, Sum, Histogram, ExponentialHistogram, Summary,
    NumberDataPoint, HistogramDataPoint, ExponentialHistogramDataPoint, SummaryDataPoint,
    AggregationTemporality,
)
from opentelemetry.proto.trace.v1.trace_pb2 import TracesData, ResourceSpans, ScopeSpans, Span, Status

# Constants
SEED = 42
TESTDATA_DIR = Path(__file__).parent.parent / "testdata"

# Realistic service pool matching current testdata
SERVICES = [
    "frontend-proxy",
    "product-catalog",
    "cart",
    "payment-service",
    "recommendation",
    "kafka",
    "analytics-service",
    "api-gateway",
]

# HTTP endpoints for realistic logs/traces
HTTP_PATHS = [
    "/api/v1/products",
    "/api/v1/cart/add",
    "/api/v1/checkout",
    "/api/v1/user/profile",
    "/health",
    "/metrics",
    "/graphql",
]

HTTP_METHODS = ["GET", "POST", "PUT", "DELETE", "PATCH"]
HTTP_STATUS_CODES = [200, 201, 204, 400, 401, 403, 404, 500, 502, 503]

@dataclass
class GenerationConfig:
    """Configuration for test data generation."""
    seed: int = SEED
    verbose: bool = False
    only: Optional[str] = None
    size_mb: Optional[float] = None  # Target file size in MB (estimates record count)


def get_output_path(base_name: str, config: GenerationConfig) -> Path:
    """Get output path with _large suffix when --size-mb is specified."""
    if config.size_mb:
        # Always add _large suffix when size is specified to distinguish from checked-in testdata
        name_parts = base_name.rsplit(".", 1)
        if len(name_parts) == 2:
            return TESTDATA_DIR / f"{name_parts[0]}_large.{name_parts[1]}"
        return TESTDATA_DIR / f"{base_name}_large"
    return TESTDATA_DIR / base_name


# Helper functions for building protobuf structures
def make_string_value(s: str) -> AnyValue:
    """Create an AnyValue with string value."""
    return AnyValue(string_value=s)


def make_int_value(i: int) -> AnyValue:
    """Create an AnyValue with int value."""
    return AnyValue(int_value=i)


def make_bool_value(b: bool) -> AnyValue:
    """Create an AnyValue with bool value."""
    return AnyValue(bool_value=b)


def make_attribute(key: str, value) -> KeyValue:
    """Create a KeyValue attribute."""
    if isinstance(value, str):
        return KeyValue(key=key, value=make_string_value(value))
    elif isinstance(value, int):
        return KeyValue(key=key, value=make_int_value(value))
    elif isinstance(value, bool):
        return KeyValue(key=key, value=make_bool_value(value))
    else:
        raise ValueError(f"Unsupported value type: {type(value)}")


def make_resource(service_name: str, **extra_attrs) -> Resource:
    """Create a Resource with common attributes."""
    attrs = [
        make_attribute("service.name", service_name),
        make_attribute("host.name", "docker-desktop"),
        make_attribute("os.type", "linux"),
        make_attribute("telemetry.sdk.language", "python"),
        make_attribute("telemetry.sdk.name", "opentelemetry"),
        make_attribute("telemetry.sdk.version", "1.37.0"),
    ]
    for k, v in extra_attrs.items():
        attrs.append(make_attribute(k, v))
    return Resource(attributes=attrs)


def random_trace_id(rng: random.Random) -> bytes:
    """Generate a random 16-byte trace ID."""
    return rng.randbytes(16)


def random_span_id(rng: random.Random) -> bytes:
    """Generate a random 8-byte span ID."""
    return rng.randbytes(8)


class LogsGenerator:
    """Generates realistic OTLP log records."""

    def __init__(self, config: GenerationConfig):
        self.config = config
        self.rng = random.Random(config.seed)
        self.base_time_ns = 1760741572254301000  # Match testdata timestamp

    def _make_log_record(
        self,
        body: str,
        severity: int = SeverityNumber.SEVERITY_NUMBER_INFO,
        attributes: List[KeyValue] = None,
        trace_id: bytes = None,
        span_id: bytes = None,
        time_offset_ns: int = 0,
    ) -> LogRecord:
        """Create a LogRecord."""
        log = LogRecord(
            time_unix_nano=self.base_time_ns + time_offset_ns,
            severity_number=severity,
            severity_text=self._severity_text(severity),
            body=make_string_value(body),
        )
        if attributes:
            log.attributes.extend(attributes)
        if trace_id:
            log.trace_id = trace_id
            log.span_id = span_id or random_span_id(self.rng)
            log.flags = 1  # Sampled
        return log

    def _severity_text(self, severity: int) -> str:
        """Map severity number to text."""
        mapping = {
            SeverityNumber.SEVERITY_NUMBER_DEBUG: "DEBUG",
            SeverityNumber.SEVERITY_NUMBER_INFO: "INFO",
            SeverityNumber.SEVERITY_NUMBER_WARN: "WARN",
            SeverityNumber.SEVERITY_NUMBER_ERROR: "ERROR",
        }
        return mapping.get(severity, "INFO")

    def generate_single_log(self, service_name: str = "frontend-proxy") -> LogsData:
        """Generate a single log record (for log.json) - Envoy access log."""
        resource = Resource(attributes=[
            make_attribute("log_name", "otel_envoy_access_log"),
            make_attribute("zone_name", ""),
            make_attribute("cluster_name", ""),
            make_attribute("node_name", ""),
            make_attribute("service.name", service_name),
            make_attribute("host.name", "docker-desktop"),
            make_attribute("os.type", "linux"),
        ])

        trace_id = random_trace_id(self.rng)
        span_id = random_span_id(self.rng)

        log_body = (
            '[2025-10-17T22:52:52.254Z] "GET /api/products/LS4PSXUNUM HTTP/1.1" '
            '200 - via_upstream - "-" 0 535 2 2 "-" "python-requests/2.32.5" '
            '"e55db68a-be46-9bb9-bb04-e431841e5b1d" "frontend-proxy:8080" '
            '"172.18.0.25:8080" frontend 172.18.0.27:53142 172.18.0.27:8080 '
            '172.18.0.26:33266 - -\n'
        )

        log_record = self._make_log_record(
            body=log_body,
            attributes=[
                make_attribute("destination.address", "172.18.0.25"),
                make_attribute("event.name", "proxy.access"),
                make_attribute("server.address", "172.18.0.27:8080"),
                make_attribute("source.address", "172.18.0.26"),
                make_attribute("upstream.cluster", "frontend"),
                make_attribute("upstream.host", "172.18.0.25:8080"),
                make_attribute("user_agent.original", "python-requests/2.32.5"),
                make_attribute("url.full", "http://frontend-proxy:8080/api/products/LS4PSXUNUM"),
                make_attribute("url.path", "/api/products/LS4PSXUNUM"),
                make_attribute("url.query", "-"),
                make_attribute("url.template", "-"),
            ],
            trace_id=trace_id,
            span_id=span_id,
        )

        scope_logs = ScopeLogs(
            scope=InstrumentationScope(),
            log_records=[log_record]
        )

        resource_logs = ResourceLogs(
            resource=resource,
            scope_logs=[scope_logs],
            schema_url="https://opentelemetry.io/schemas/1.6.1"
        )

        return LogsData(resource_logs=[resource_logs])

    def generate_batch_logs(self, count: int = 81) -> List[LogsData]:
        """Generate multiple log records (for logs.jsonl) - varied realistic logs."""
        # If size_mb is specified, estimate count based on ~1.5KB per log record
        if self.config.size_mb:
            estimated_bytes = self.config.size_mb * 1024 * 1024
            avg_record_size = 1500  # Average bytes per log record (conservative estimate)
            count = max(1, int(estimated_bytes / avg_record_size))
            if self.config.verbose:
                print(f"  Generating ~{count} log records to reach ~{self.config.size_mb}MB")

        logs_list = []

        # Generate varied logs from different services
        log_patterns = [
            ("load-generator", "User browsing product: {}", SeverityNumber.SEVERITY_NUMBER_INFO, [
                make_attribute("code.file.path", "/usr/src/app/locustfile.py"),
                make_attribute("code.function.name", "browse_product"),
                make_attribute("code.line.number", 130),
            ]),
            ("product-catalog", "Product Found", SeverityNumber.SEVERITY_NUMBER_INFO, [
                make_attribute("app.product.name", "The Comet Book"),
                make_attribute("app.product.id", "HQTGWGPNH4"),
            ]),
            ("recommendation", "Receive ListRecommendations for product ids:['HQTGWGPNH4']", SeverityNumber.SEVERITY_NUMBER_INFO, [
                make_attribute("code.file.path", "/app/recommendation_server.py"),
                make_attribute("code.function.name", "ListRecommendations"),
                make_attribute("code.line.number", 47),
            ]),
            ("frontend-proxy", '[2025-10-17T22:52:35.579Z] "GET /api/products/0PUK6V6EV0 HTTP/1.1" 200 - via_upstream - "-" 0 421 3 3', SeverityNumber.SEVERITY_NUMBER_INFO, [
                make_attribute("destination.address", "172.18.0.25"),
                make_attribute("event.name", "proxy.access"),
                make_attribute("url.path", "/api/products/0PUK6V6EV0"),
            ]),
            ("cart", "GetCartAsync called with userId=", SeverityNumber.SEVERITY_NUMBER_INFO, []),
            ("kafka", "[ProducerStateManager partition=__cluster_metadata-0] Wrote producer snapshot", SeverityNumber.SEVERITY_NUMBER_INFO, []),
        ]

        for i in range(count):
            service, body_template, severity, base_attrs = self.rng.choice(log_patterns)
            body = body_template.format(self.rng.choice(["HQTGWGPNH4", "LS4PSXUNUM", "1YMWWN1N4O"])) if "{}" in body_template else body_template

            resource = make_resource(
                service,
                **{"service.namespace": "opentelemetry-demo", "service.version": "2.1.3"}
            )

            # 30% of logs have trace context
            trace_id = random_trace_id(self.rng) if self.rng.random() < 0.3 else None
            span_id = random_span_id(self.rng) if trace_id else None

            log_record = self._make_log_record(
                body=body,
                severity=severity,
                attributes=base_attrs.copy(),
                trace_id=trace_id,
                span_id=span_id,
                time_offset_ns=i * 1_000_000_000,  # 1 second apart
            )

            scope_logs = ScopeLogs(
                scope=InstrumentationScope(name=service),
                log_records=[log_record]
            )

            resource_logs = ResourceLogs(
                resource=resource,
                scope_logs=[scope_logs],
                schema_url="https://opentelemetry.io/schemas/1.6.1"
            )

            logs_list.append(LogsData(resource_logs=[resource_logs]))

        return logs_list


class MetricsGenerator:
    """Generates realistic OTLP metrics using OpenTelemetry SDK."""

    def __init__(self, config: GenerationConfig):
        self.config = config
        self.rng = random.Random(config.seed)
        self.base_time_ns = 1705327800000000000  # Base timestamp from spec

    def _scale_datapoints(self, base_count: int) -> int:
        """Scale number of data points based on target size."""
        if not self.config.size_mb:
            return base_count
        # Estimate ~800 bytes per metric data point (conservative)
        estimated_bytes = self.config.size_mb * 1024 * 1024
        avg_size = 800
        scaled = max(1, int(estimated_bytes / avg_size))
        if self.config.verbose:
            print(f"  Scaling from {base_count} to ~{scaled} data points for ~{self.config.size_mb}MB")
        return scaled

    def generate_gauge(self) -> MetricsData:
        """Generate gauge metrics (instant measurements)."""
        resource = Resource(attributes=[
            make_attribute("service.name", "demo-service"),
            make_attribute("service.version", "1.0.0"),
            make_attribute("deployment.environment", "production"),
        ])

        # CPU usage gauge with multiple data points
        num_points = self._scale_datapoints(2)
        cpu_data_points = []
        for i in range(num_points):
            cpu_data_points.append(NumberDataPoint(
                attributes=[
                    make_attribute("host", f"web-{i // 8 + 1:02d}"),
                    make_attribute("cpu", i % 8),
                ],
                time_unix_nano=self.base_time_ns + (i * 1_000_000_000),
                as_double=self.rng.uniform(20.0, 95.0),
            ))
        cpu_gauge = Gauge(data_points=cpu_data_points)

        # Memory available gauge
        memory_gauge = Gauge(data_points=[
            NumberDataPoint(
                attributes=[make_attribute("host", "web-01")],
                time_unix_nano=self.base_time_ns,
                as_int=8589934592,  # 8GB
            ),
        ])

        metrics = [
            Metric(
                name="cpu.usage",
                description="Current CPU usage percentage",
                unit="percent",
                gauge=cpu_gauge,
            ),
            Metric(
                name="memory.available",
                description="Available memory in bytes",
                unit="bytes",
                gauge=memory_gauge,
            ),
        ]

        scope_metrics = ScopeMetrics(
            scope=InstrumentationScope(name="demo-instrumentation", version="1.0.0"),
            metrics=metrics,
        )

        resource_metrics = ResourceMetrics(
            resource=resource,
            scope_metrics=[scope_metrics],
        )

        return MetricsData(resource_metrics=[resource_metrics])

    def generate_sum(self) -> MetricsData:
        """Generate sum metrics (cumulative/delta aggregations)."""
        resource = Resource(attributes=[
            make_attribute("service.name", "api-gateway"),
            make_attribute("service.version", "2.1.0"),
        ])

        # HTTP requests counter
        http_sum = Sum(
            aggregation_temporality=AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE,
            is_monotonic=True,
            data_points=[
                NumberDataPoint(
                    attributes=[
                        make_attribute("http.method", "GET"),
                        make_attribute("http.status_code", 200),
                        make_attribute("http.route", "/api/users"),
                    ],
                    time_unix_nano=self.base_time_ns,
                    as_int=15234,
                ),
                NumberDataPoint(
                    attributes=[
                        make_attribute("http.method", "POST"),
                        make_attribute("http.status_code", 201),
                        make_attribute("http.route", "/api/users"),
                    ],
                    time_unix_nano=self.base_time_ns,
                    as_int=3421,
                ),
                NumberDataPoint(
                    attributes=[
                        make_attribute("http.method", "GET"),
                        make_attribute("http.status_code", 404),
                        make_attribute("http.route", "/api/users"),
                    ],
                    time_unix_nano=self.base_time_ns,
                    as_int=152,
                ),
            ],
        )

        # Bytes received counter
        bytes_sum = Sum(
            aggregation_temporality=AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE,
            is_monotonic=True,
            data_points=[
                NumberDataPoint(
                    attributes=[make_attribute("http.route", "/api/users")],
                    time_unix_nano=self.base_time_ns,
                    as_int=45892134,
                ),
            ],
        )

        metrics = [
            Metric(
                name="http.requests.total",
                description="Total number of HTTP requests",
                unit="1",
                sum=http_sum,
            ),
            Metric(
                name="http.request.bytes",
                description="Total bytes received in requests",
                unit="bytes",
                sum=bytes_sum,
            ),
        ]

        scope_metrics = ScopeMetrics(
            scope=InstrumentationScope(name="http-instrumentation", version="1.2.0"),
            metrics=metrics,
        )

        resource_metrics = ResourceMetrics(
            resource=resource,
            scope_metrics=[scope_metrics],
        )

        return MetricsData(resource_metrics=[resource_metrics])

    def generate_histogram(self) -> MetricsData:
        """Generate histogram metrics (distributions with explicit buckets)."""
        resource = Resource(attributes=[
            make_attribute("service.name", "api-gateway"),
            make_attribute("deployment.environment", "production"),
        ])

        # HTTP duration histogram
        http_histogram = Histogram(
            aggregation_temporality=AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE,
            data_points=[
                HistogramDataPoint(
                    attributes=[
                        make_attribute("http.method", "GET"),
                        make_attribute("http.route", "/api/users"),
                        make_attribute("http.status_code", 200),
                    ],
                    time_unix_nano=self.base_time_ns,
                    count=1543,
                    sum=45120.5,
                    bucket_counts=[23, 342, 687, 398, 78, 15],
                    explicit_bounds=[10.0, 25.0, 50.0, 100.0, 250.0],
                    min=2.3,
                    max=421.8,
                ),
                HistogramDataPoint(
                    attributes=[
                        make_attribute("http.method", "POST"),
                        make_attribute("http.route", "/api/users"),
                        make_attribute("http.status_code", 201),
                    ],
                    time_unix_nano=self.base_time_ns,
                    count=892,
                    sum=67854.2,
                    bucket_counts=[12, 156, 298, 321, 89, 16],
                    explicit_bounds=[10.0, 25.0, 50.0, 100.0, 250.0],
                    min=5.1,
                    max=512.3,
                ),
            ],
        )

        # Database query histogram
        db_histogram = Histogram(
            aggregation_temporality=AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE,
            data_points=[
                HistogramDataPoint(
                    attributes=[
                        make_attribute("db.system", "postgresql"),
                        make_attribute("db.operation", "SELECT"),
                    ],
                    time_unix_nano=self.base_time_ns,
                    count=5234,
                    sum=12456.8,
                    bucket_counts=[1234, 2891, 876, 198, 35],
                    explicit_bounds=[1.0, 5.0, 10.0, 50.0],
                    min=0.2,
                    max=89.5,
                ),
            ],
        )

        metrics = [
            Metric(
                name="http.server.duration",
                description="Duration of HTTP requests in milliseconds",
                unit="ms",
                histogram=http_histogram,
            ),
            Metric(
                name="db.query.duration",
                description="Duration of database queries",
                unit="ms",
                histogram=db_histogram,
            ),
        ]

        scope_metrics = ScopeMetrics(
            scope=InstrumentationScope(name="http-instrumentation", version="1.2.0"),
            metrics=metrics,
        )

        resource_metrics = ResourceMetrics(
            resource=resource,
            scope_metrics=[scope_metrics],
        )

        return MetricsData(resource_metrics=[resource_metrics])

    def generate_exponential_histogram(self) -> MetricsData:
        """Generate exponential histogram metrics."""
        resource = Resource(attributes=[
            make_attribute("service.name", "payment-service"),
            make_attribute("service.version", "3.2.1"),
        ])

        exp_histogram = ExponentialHistogram(
            aggregation_temporality=AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE,
            data_points=[
                ExponentialHistogramDataPoint(
                    attributes=[
                        make_attribute("payment.method", "credit_card"),
                        make_attribute("payment.provider", "stripe"),
                    ],
                    time_unix_nano=self.base_time_ns,
                    count=2543,
                    sum=125634.5,
                    scale=3,
                    zero_count=5,
                    positive=ExponentialHistogramDataPoint.Buckets(
                        offset=2,
                        bucket_counts=[234, 567, 892, 645, 178, 22],
                    ),
                    negative=ExponentialHistogramDataPoint.Buckets(
                        offset=0,
                        bucket_counts=[],
                    ),
                    min=12.3,
                    max=1234.5,
                ),
                ExponentialHistogramDataPoint(
                    attributes=[
                        make_attribute("payment.method", "paypal"),
                        make_attribute("payment.provider", "paypal"),
                    ],
                    time_unix_nano=self.base_time_ns,
                    count=1832,
                    sum=98234.2,
                    scale=3,
                    zero_count=3,
                    positive=ExponentialHistogramDataPoint.Buckets(
                        offset=1,
                        bucket_counts=[156, 432, 721, 412, 98, 12],
                    ),
                    negative=ExponentialHistogramDataPoint.Buckets(
                        offset=0,
                        bucket_counts=[],
                    ),
                    min=8.7,
                    max=892.3,
                ),
            ],
        )

        metrics = [
            Metric(
                name="payment.processing.duration",
                description="Duration of payment processing operations",
                unit="ms",
                exponential_histogram=exp_histogram,
            ),
        ]

        scope_metrics = ScopeMetrics(
            scope=InstrumentationScope(name="payment-instrumentation", version="1.0.0"),
            metrics=metrics,
        )

        resource_metrics = ResourceMetrics(
            resource=resource,
            scope_metrics=[scope_metrics],
        )

        return MetricsData(resource_metrics=[resource_metrics])

    def generate_summary(self) -> MetricsData:
        """Generate summary metrics (quantile-based distributions)."""
        resource = Resource(attributes=[
            make_attribute("service.name", "analytics-service"),
            make_attribute("service.version", "1.5.2"),
        ])

        summary = Summary(data_points=[
            SummaryDataPoint(
                attributes=[
                    make_attribute("endpoint", "/api/analytics"),
                    make_attribute("method", "POST"),
                ],
                time_unix_nano=self.base_time_ns,
                count=8534,
                sum=45892134.0,
                quantile_values=[
                    SummaryDataPoint.ValueAtQuantile(quantile=0.0, value=128.0),
                    SummaryDataPoint.ValueAtQuantile(quantile=0.5, value=4512.0),
                    SummaryDataPoint.ValueAtQuantile(quantile=0.9, value=12834.0),
                    SummaryDataPoint.ValueAtQuantile(quantile=0.95, value=18923.0),
                    SummaryDataPoint.ValueAtQuantile(quantile=0.99, value=45123.0),
                    SummaryDataPoint.ValueAtQuantile(quantile=1.0, value=98234.0),
                ],
            ),
            SummaryDataPoint(
                attributes=[
                    make_attribute("endpoint", "/api/data"),
                    make_attribute("method", "GET"),
                ],
                time_unix_nano=self.base_time_ns,
                count=12934,
                sum=12345678.0,
                quantile_values=[
                    SummaryDataPoint.ValueAtQuantile(quantile=0.0, value=64.0),
                    SummaryDataPoint.ValueAtQuantile(quantile=0.5, value=892.0),
                    SummaryDataPoint.ValueAtQuantile(quantile=0.9, value=2341.0),
                    SummaryDataPoint.ValueAtQuantile(quantile=0.95, value=3456.0),
                    SummaryDataPoint.ValueAtQuantile(quantile=0.99, value=8234.0),
                    SummaryDataPoint.ValueAtQuantile(quantile=1.0, value=15234.0),
                ],
            ),
        ])

        metrics = [
            Metric(
                name="request.size",
                description="Summary of request sizes",
                unit="bytes",
                summary=summary,
            ),
        ]

        scope_metrics = ScopeMetrics(
            scope=InstrumentationScope(name="analytics-instrumentation", version="2.0.0"),
            metrics=metrics,
        )

        resource_metrics = ResourceMetrics(
            resource=resource,
            scope_metrics=[scope_metrics],
        )

        return MetricsData(resource_metrics=[resource_metrics])

    def generate_mixed(self) -> MetricsData:
        """Generate mixed metrics (multiple types in one file)."""
        resource = Resource(attributes=[
            make_attribute("service.name", "multi-service"),
            make_attribute("service.version", "1.0.0"),
            make_attribute("deployment.environment", "staging"),
        ])

        # Gauge metric
        cpu_gauge = Gauge(data_points=[
            NumberDataPoint(
                attributes=[make_attribute("cpu", 0)],
                time_unix_nano=self.base_time_ns,
                as_double=0.42,
            ),
        ])

        # Sum metric
        network_sum = Sum(
            aggregation_temporality=AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE,
            is_monotonic=True,
            data_points=[
                NumberDataPoint(
                    attributes=[make_attribute("direction", "transmit")],
                    time_unix_nano=self.base_time_ns,
                    as_int=123456789,
                ),
            ],
        )

        # Histogram metric
        http_histogram = Histogram(
            aggregation_temporality=AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE,
            data_points=[
                HistogramDataPoint(
                    attributes=[make_attribute("http.method", "GET")],
                    time_unix_nano=self.base_time_ns,
                    count=100,
                    sum=5000.0,
                    bucket_counts=[10, 30, 40, 15, 5],
                    explicit_bounds=[10.0, 50.0, 100.0, 500.0],
                    min=5.0,
                    max=1000.0,
                ),
            ],
        )

        metrics = [
            Metric(
                name="system.cpu.utilization",
                description="CPU utilization",
                unit="1",
                gauge=cpu_gauge,
            ),
            Metric(
                name="system.network.io",
                description="Network bytes transferred",
                unit="bytes",
                sum=network_sum,
            ),
            Metric(
                name="http.server.request.duration",
                description="HTTP request duration",
                unit="ms",
                histogram=http_histogram,
            ),
        ]

        scope_metrics = ScopeMetrics(
            scope=InstrumentationScope(name="system-instrumentation", version="1.0.0"),
            metrics=metrics,
        )

        resource_metrics = ResourceMetrics(
            resource=resource,
            scope_metrics=[scope_metrics],
        )

        return MetricsData(resource_metrics=[resource_metrics])


class TracesGenerator:
    """Generates realistic OTLP trace spans using OpenTelemetry SDK."""

    def __init__(self, config: GenerationConfig):
        self.config = config
        self.rng = random.Random(config.seed)
        self.base_time_ns = 1760738064624180000  # Base timestamp from spec

    def generate_single_trace(self) -> TracesData:
        """Generate a single trace with 2 spans (for trace.json)."""
        trace_id = random_trace_id(self.rng)
        parent_span_id = random_span_id(self.rng)
        child_span_id = random_span_id(self.rng)
        root_span_id = random_span_id(self.rng)

        resource = Resource(attributes=[
            make_attribute("service.name", "frontend-proxy"),
            make_attribute("service.namespace", "opentelemetry-demo"),
            make_attribute("service.version", "2.1.3"),
            make_attribute("telemetry.sdk.language", "cpp"),
            make_attribute("telemetry.sdk.name", "envoy"),
            make_attribute("telemetry.sdk.version", "e98143bdac2bd4983e889b89d57330ca59cd71b0/1.34.7/Clean/RELEASE/BoringSSL"),
            make_attribute("host.name", "docker-desktop"),
            make_attribute("os.type", "linux"),
        ])

        # Child span (egress)
        child_span = Span(
            trace_id=trace_id,
            span_id=child_span_id,
            parent_span_id=parent_span_id,
            name="router frontend egress",
            kind=Span.SpanKind.SPAN_KIND_CLIENT,
            start_time_unix_nano=self.base_time_ns + 282000,
            end_time_unix_nano=self.base_time_ns + 4303000,
            attributes=[
                make_attribute("http.protocol", "HTTP/1.1"),
                make_attribute("upstream_address", "172.18.0.25:8080"),
                make_attribute("peer.address", "172.18.0.25:8080"),
                make_attribute("component", "proxy"),
                make_attribute("upstream_cluster", "frontend"),
                make_attribute("upstream_cluster.name", "frontend"),
                make_attribute("http.status_code", "200"),
                make_attribute("response_flags", "-"),
            ],
            status=Status(),
        )

        # Parent span (ingress)
        parent_span = Span(
            trace_id=trace_id,
            span_id=parent_span_id,
            parent_span_id=root_span_id,
            name="ingress",
            kind=Span.SpanKind.SPAN_KIND_SERVER,
            start_time_unix_nano=self.base_time_ns,
            end_time_unix_nano=self.base_time_ns + 4328000,
            attributes=[
                make_attribute("node_id", ""),
                make_attribute("zone", ""),
                make_attribute("guid:x-request-id", "4fb197b8-8116-9929-a374-8b4813c60ad2"),
                make_attribute("http.url", "http://frontend-proxy:8080/api/products/66VCHSJNUP"),
                make_attribute("http.method", "GET"),
                make_attribute("downstream_cluster", "-"),
                make_attribute("user_agent", "python-requests/2.32.5"),
                make_attribute("http.protocol", "HTTP/1.1"),
                make_attribute("peer.address", "172.18.0.26"),
                make_attribute("request_size", "0"),
                make_attribute("response_size", "498"),
                make_attribute("component", "proxy"),
                make_attribute("upstream_cluster", "frontend"),
                make_attribute("upstream_cluster.name", "frontend"),
                make_attribute("http.status_code", "200"),
                make_attribute("response_flags", "-"),
            ],
            status=Status(),
        )

        scope_spans = ScopeSpans(
            scope=InstrumentationScope(
                name="envoy",
                version="e98143bdac2bd4983e889b89d57330ca59cd71b0/1.34.7/Clean/RELEASE/BoringSSL"
            ),
            spans=[child_span, parent_span]
        )

        resource_spans = ResourceSpans(
            resource=resource,
            scope_spans=[scope_spans],
            schema_url="https://opentelemetry.io/schemas/1.6.1"
        )

        return TracesData(resource_spans=[resource_spans])

    def generate_batch_traces(self, count: int = 19) -> List[TracesData]:
        """Generate multiple trace spans (for traces.jsonl)."""
        # If size_mb is specified, estimate count based on ~2KB per trace span
        if self.config.size_mb:
            estimated_bytes = self.config.size_mb * 1024 * 1024
            avg_record_size = 2000  # Average bytes per trace span (conservative estimate)
            count = max(1, int(estimated_bytes / avg_record_size))
            if self.config.verbose:
                print(f"  Generating ~{count} trace spans to reach ~{self.config.size_mb}MB")

        traces_list = []

        # Generate varied traces from different services
        for i in range(count):
            service = self.rng.choice(SERVICES)
            trace_id = random_trace_id(self.rng)
            span_id = random_span_id(self.rng)
            parent_span_id = random_span_id(self.rng) if self.rng.random() < 0.7 else b""

            # Pick span kind
            kind = self.rng.choice([
                Span.SpanKind.SPAN_KIND_SERVER,
                Span.SpanKind.SPAN_KIND_CLIENT,
                Span.SpanKind.SPAN_KIND_INTERNAL,
            ])

            # Build realistic attributes based on service
            attributes = []
            if "proxy" in service or "gateway" in service:
                attributes.extend([
                    make_attribute("http.method", self.rng.choice(HTTP_METHODS)),
                    make_attribute("http.url", f"http://{service}:8080{self.rng.choice(HTTP_PATHS)}"),
                    make_attribute("http.status_code", str(self.rng.choice(HTTP_STATUS_CODES))),
                    make_attribute("component", "proxy"),
                ])
            elif "catalog" in service or "cart" in service or "payment" in service:
                attributes.extend([
                    make_attribute("rpc.system", "grpc"),
                    make_attribute("rpc.service", f"oteldemo.{service.title()}Service"),
                    make_attribute("rpc.method", self.rng.choice(["Get", "List", "Update"])),
                    make_attribute("rpc.grpc.status_code", 0),
                ])
            else:
                attributes.extend([
                    make_attribute("service.name", service),
                    make_attribute("operation", self.rng.choice(["query", "process", "transform"])),
                ])

            # Create span with realistic timing
            start_time = self.base_time_ns + (i * 1_000_000_000)  # 1 second apart
            duration_ms = self.rng.randint(1, 500)  # 1-500ms
            end_time = start_time + (duration_ms * 1_000_000)

            span = Span(
                trace_id=trace_id,
                span_id=span_id,
                parent_span_id=parent_span_id,
                name=f"{service}.{self.rng.choice(['handle', 'process', 'execute'])}",
                kind=kind,
                start_time_unix_nano=start_time,
                end_time_unix_nano=end_time,
                attributes=attributes,
                status=Status(),
            )

            # Add occasional events
            if self.rng.random() < 0.3:
                span.events.append(Span.Event(
                    time_unix_nano=start_time + (duration_ms * 500_000),
                    name=self.rng.choice(["checkpoint", "processing", "validation"]),
                ))

            resource = make_resource(
                service,
                **{
                    "service.namespace": "opentelemetry-demo",
                    "service.version": "2.1.3",
                }
            )

            scope_spans = ScopeSpans(
                scope=InstrumentationScope(name=service),
                spans=[span]
            )

            resource_spans = ResourceSpans(
                resource=resource,
                scope_spans=[scope_spans],
                schema_url="https://opentelemetry.io/schemas/1.6.1"
            )

            traces_list.append(TracesData(resource_spans=[resource_spans]))

        return traces_list


def export_to_json(data, output_path: Path, indent: int = 2):
    """Export OTLP data to JSON format."""
    json_str = json_format.MessageToJson(
        data,
        indent=indent,
        preserving_proto_field_name=False,  # Use camelCase
        always_print_fields_with_no_presence=False,
    )
    output_path.write_text(json_str)


def export_to_jsonl(records: List, output_path: Path):
    """Export OTLP records to JSONL format (one record per line)."""
    with output_path.open("w") as f:
        for record in records:
            json_str = json_format.MessageToJson(
                record,
                indent=0,  # No indent for JSONL
                preserving_proto_field_name=False,
                always_print_fields_with_no_presence=False,
            )
            # Remove newlines from the JSON string
            json_str = json_str.replace("\n", "").replace("  ", "")
            f.write(json_str + "\n")


def export_to_protobuf(data, output_path: Path):
    """Export OTLP data to protobuf binary format."""
    if isinstance(data, list):
        # For batch data, we need to combine into single message
        # This is used for logs.pb and traces.pb which have multiple records
        if len(data) > 0:
            if isinstance(data[0], LogsData):
                # Combine multiple LogsData into one
                combined = LogsData()
                for log_data in data:
                    combined.resource_logs.extend(log_data.resource_logs)
                output_path.write_bytes(combined.SerializeToString())
            elif isinstance(data[0], TracesData):
                # Combine multiple TracesData into one
                combined = TracesData()
                for trace_data in data:
                    combined.resource_spans.extend(trace_data.resource_spans)
                output_path.write_bytes(combined.SerializeToString())
            else:
                # Unknown type, just serialize first
                output_path.write_bytes(data[0].SerializeToString())
    else:
        # Single message
        output_path.write_bytes(data.SerializeToString())


def generate_all_logs(config: GenerationConfig):
    """Generate all log files."""
    print("Generating logs...")
    generator = LogsGenerator(config)

    if config.size_mb:
        # For size-specified generation, only generate batch logs
        batch_logs = generator.generate_batch_logs()
        export_to_jsonl(batch_logs, get_output_path("logs.jsonl", config))
        export_to_protobuf(batch_logs, get_output_path("logs.pb", config))
        print(f"  ✓ logs_large.{{jsonl,pb}} (~{config.size_mb}MB)")
    else:
        # Standard small testdata
        single_log = generator.generate_single_log()
        export_to_json(single_log, TESTDATA_DIR / "log.json")

        batch_logs = generator.generate_batch_logs(81)
        export_to_jsonl(batch_logs, TESTDATA_DIR / "logs.jsonl")
        export_to_protobuf(batch_logs, TESTDATA_DIR / "logs.pb")

        print("  ✓ log.json, logs.jsonl, logs.pb")


def generate_all_metrics(config: GenerationConfig):
    """Generate all metric files."""
    print("Generating metrics...")
    generator = MetricsGenerator(config)

    metric_types = [
        ("gauge", generator.generate_gauge),
        ("sum", generator.generate_sum),
        ("histogram", generator.generate_histogram),
        ("exponential_histogram", generator.generate_exponential_histogram),
        ("summary", generator.generate_summary),
    ]

    for type_name, gen_func in metric_types:
        data = gen_func()
        base_name = f"metrics_{type_name}"

        export_to_json(data, TESTDATA_DIR / f"{base_name}.json")
        export_to_jsonl([data], TESTDATA_DIR / f"{base_name}.jsonl")
        export_to_protobuf(data, TESTDATA_DIR / f"{base_name}.pb")

        print(f"  ✓ {base_name}.{{json,jsonl,pb}}")

    # Mixed metrics
    mixed_data = generator.generate_mixed()
    export_to_json(mixed_data, TESTDATA_DIR / "metrics_mixed.json")
    export_to_jsonl([mixed_data], TESTDATA_DIR / "metrics_mixed.jsonl")
    export_to_protobuf(mixed_data, TESTDATA_DIR / "metrics_mixed.pb")
    print("  ✓ metrics_mixed.{json,jsonl,pb}")


def generate_all_traces(config: GenerationConfig):
    """Generate all trace files."""
    print("Generating traces...")
    generator = TracesGenerator(config)

    if config.size_mb:
        # For size-specified generation, only generate batch traces
        batch_traces = generator.generate_batch_traces()
        export_to_jsonl(batch_traces, get_output_path("traces.jsonl", config))
        export_to_protobuf(batch_traces, get_output_path("traces.pb", config))
        print(f"  ✓ traces_large.{{jsonl,pb}} (~{config.size_mb}MB)")
    else:
        # Standard small testdata
        single_trace = generator.generate_single_trace()
        export_to_json(single_trace, TESTDATA_DIR / "trace.json")
        export_to_protobuf(single_trace, TESTDATA_DIR / "trace.pb")

        batch_traces = generator.generate_batch_traces(19)
        export_to_jsonl(batch_traces, TESTDATA_DIR / "traces.jsonl")
        export_to_protobuf(batch_traces, TESTDATA_DIR / "traces.pb")

        print("  ✓ trace.json, trace.pb, traces.jsonl, traces.pb")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Generate deterministic OpenTelemetry test data"
    )
    parser.add_argument(
        "--only",
        choices=[
            "logs", "traces", "metrics",
            "metrics-gauge", "metrics-sum", "metrics-histogram",
            "metrics-exponential-histogram", "metrics-summary", "metrics-mixed"
        ],
        help="Generate only specific signal type or metric type"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose output"
    )
    parser.add_argument(
        "--size-mb",
        type=float,
        help="Target file size in MB (generates enough records to approximate this size). "
             "Files are named with _large suffix and excluded from git via .gitignore"
    )

    args = parser.parse_args()
    config = GenerationConfig(
        seed=SEED,
        verbose=args.verbose,
        only=args.only,
        size_mb=args.size_mb
    )

    print(f"🔧 Generating OpenTelemetry test data (seed={SEED})")
    print(f"📁 Output directory: {TESTDATA_DIR}")
    print()

    start_time = time.time()

    try:
        if config.only is None:
            # Generate all
            generate_all_logs(config)
            generate_all_metrics(config)
            generate_all_traces(config)
        elif config.only == "logs":
            generate_all_logs(config)
        elif config.only == "traces":
            generate_all_traces(config)
        elif config.only == "metrics":
            generate_all_metrics(config)
        elif config.only.startswith("metrics-"):
            # Specific metric type
            metric_type = config.only.replace("metrics-", "")
            print(f"Generating metrics ({metric_type})...")
            generator = MetricsGenerator(config)

            gen_func = getattr(generator, f"generate_{metric_type}")
            data = gen_func()
            base_name = f"metrics_{metric_type}"

            export_to_json(data, TESTDATA_DIR / f"{base_name}.json")
            export_to_jsonl([data], TESTDATA_DIR / f"{base_name}.jsonl")
            export_to_protobuf(data, TESTDATA_DIR / f"{base_name}.pb")

            print(f"  ✓ {base_name}.{{json,jsonl,pb}}")

        elapsed = time.time() - start_time
        print()
        print(f"✅ Test data generation complete in {elapsed:.2f}s")
        return 0

    except Exception as e:
        print(f"❌ Error: {e}", file=sys.stderr)
        if config.verbose:
            import traceback
            traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())

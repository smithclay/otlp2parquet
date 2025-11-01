// Build script for generating Rust code from protobuf definitions
//
// This will compile the OpenTelemetry proto files when they are added.
// For now, it's a placeholder that will be updated when we add the proto files.

fn main() {
    // Compile OpenTelemetry proto files
    // Using prost-build for pure protobuf message types (no gRPC)
    prost_build::Config::new()
        // Enable serde derives for JSON support
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        // Note: Not using #[serde(default)] as it breaks enum variants
        .compile_protos(
            &[
                "proto/opentelemetry/proto/collector/logs/v1/logs_service.proto",
                "proto/opentelemetry/proto/logs/v1/logs.proto",
                "proto/opentelemetry/proto/collector/trace/v1/trace_service.proto",
                "proto/opentelemetry/proto/trace/v1/trace.proto",
                "proto/opentelemetry/proto/collector/metrics/v1/metrics_service.proto",
                "proto/opentelemetry/proto/metrics/v1/metrics.proto",
                "proto/opentelemetry/proto/common/v1/common.proto",
                "proto/opentelemetry/proto/resource/v1/resource.proto",
            ],
            &["proto/"],
        )
        .unwrap();

    println!("cargo:rerun-if-changed=proto/");
}

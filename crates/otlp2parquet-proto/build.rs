// Build script for generating Rust code from protobuf definitions
//
// This will compile the OpenTelemetry proto files when they are added.
// For now, it's a placeholder that will be updated when we add the proto files.

fn main() {
    // Compile OpenTelemetry proto files
    // We don't need server or client stubs (just the message types)
    tonic_build::configure()
        .build_server(false)
        .build_client(false)
        .compile_protos(
            &[
                "proto/opentelemetry/proto/collector/logs/v1/logs_service.proto",
                "proto/opentelemetry/proto/logs/v1/logs.proto",
                "proto/opentelemetry/proto/common/v1/common.proto",
                "proto/opentelemetry/proto/resource/v1/resource.proto",
            ],
            &["proto/"],
        )
        .unwrap();

    println!("cargo:rerun-if-changed=proto/");
}

// otlp2parquet-proto - OpenTelemetry Protocol Definitions
//
// This crate contains the generated protobuf code for OTLP.
// The code is generated at build time from the OpenTelemetry proto files.

// Include generated protobuf code
pub mod opentelemetry {
    pub mod proto {
        pub mod collector {
            pub mod logs {
                pub mod v1 {
                    include!(concat!(
                        env!("OUT_DIR"),
                        "/opentelemetry.proto.collector.logs.v1.rs"
                    ));
                }
            }
            pub mod trace {
                pub mod v1 {
                    include!(concat!(
                        env!("OUT_DIR"),
                        "/opentelemetry.proto.collector.trace.v1.rs"
                    ));
                }
            }
            pub mod metrics {
                pub mod v1 {
                    include!(concat!(
                        env!("OUT_DIR"),
                        "/opentelemetry.proto.collector.metrics.v1.rs"
                    ));
                }
            }
        }
        pub mod logs {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/opentelemetry.proto.logs.v1.rs"));
            }
        }
        pub mod trace {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/opentelemetry.proto.trace.v1.rs"));
            }
        }
        pub mod metrics {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/opentelemetry.proto.metrics.v1.rs"));
            }
        }
        pub mod common {
            pub mod v1 {
                include!(concat!(
                    env!("OUT_DIR"),
                    "/opentelemetry.proto.common.v1.rs"
                ));
            }
        }
        pub mod resource {
            pub mod v1 {
                include!(concat!(
                    env!("OUT_DIR"),
                    "/opentelemetry.proto.resource.v1.rs"
                ));
            }
        }
    }
}

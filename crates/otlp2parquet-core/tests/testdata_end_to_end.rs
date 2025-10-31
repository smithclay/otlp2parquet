use std::fs;
use std::path::Path;

use arrow::array::{Array, StringArray, StructArray, TimestampNanosecondArray};
use arrow::record_batch::RecordBatch;
use otlp2parquet_core::{process_otlp_logs, process_otlp_logs_with_format, InputFormat};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use prost::bytes::Bytes;

fn load_testdata_path(filename: &str) -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("testdata")
        .join(filename)
}

fn read_first_batch(bytes: &[u8]) -> RecordBatch {
    let bytes = Bytes::from(bytes.to_vec());
    ParquetRecordBatchReaderBuilder::try_new(bytes)
        .expect("failed to create parquet reader")
        .build()
        .expect("failed to build parquet reader")
        .next()
        .expect("parquet file should contain at least one batch")
        .expect("failed to read record batch")
}

#[test]
fn protobuf_testdata_round_trip() {
    let pb_path = load_testdata_path("logs.pb");
    let protobuf_bytes = fs::read(pb_path).expect("failed to read logs.pb");

    let protobuf_result =
        process_otlp_logs(&protobuf_bytes).expect("protobuf processing of testdata");

    assert_eq!(
        &protobuf_result.parquet_bytes[0..4],
        b"PAR1",
        "protobuf results should be valid parquet"
    );

    let protobuf_batch = read_first_batch(&protobuf_result.parquet_bytes);

    // Sanity-check metadata extracted on the real payload.
    assert!(
        protobuf_result.timestamp_nanos > 0,
        "timestamp should be present in protobuf metadata"
    );
    assert!(
        !protobuf_result.service_name.is_empty(),
        "service name should be extracted from resource attributes"
    );

    // Validate a few core columns from the record batch.
    let timestamp_col = protobuf_batch
        .column(0)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .expect("Timestamp column should be TimestampNanosecondArray");
    assert!(
        (0..timestamp_col.len())
            .any(|idx| timestamp_col.value(idx) == protobuf_result.timestamp_nanos),
        "at least one row should match the metadata timestamp"
    );

    let service_col = protobuf_batch
        .column(8)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("ServiceName column should be StringArray");
    assert!(
        service_col
            .iter()
            .any(|value| value == Some(protobuf_result.service_name.as_str())),
        "service name column should contain the extracted metadata value"
    );

    let body_col = protobuf_batch
        .column(7)
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("Body column should be StructArray");
    let body_strings = body_col
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Body.StringValue should be StringArray");
    assert!(
        !body_strings.is_empty() && !body_strings.value(0).is_empty(),
        "log body should contain string data"
    );
}

#[test]
fn json_testdata_round_trip() {
    let json_path = load_testdata_path("log.json");
    let json_bytes = fs::read(json_path).expect("failed to read log.json");

    let result = process_otlp_logs_with_format(&json_bytes, InputFormat::Json)
        .expect("json processing of testdata");

    assert_eq!(&result.parquet_bytes[0..4], b"PAR1");
    assert!(
        result.timestamp_nanos > 0,
        "timestamp should be extracted from JSON payload"
    );
    assert!(
        !result.service_name.is_empty(),
        "service name should be present in JSON metadata"
    );

    let batch = read_first_batch(&result.parquet_bytes);
    assert!(batch.num_rows() > 0, "JSON payload should yield rows");
}

#[test]
fn jsonl_testdata_round_trip() {
    let jsonl_path = load_testdata_path("logs.jsonl");
    let jsonl_bytes = fs::read(jsonl_path).expect("failed to read logs.jsonl");

    let result = process_otlp_logs_with_format(&jsonl_bytes, InputFormat::Jsonl)
        .expect("jsonl processing of testdata");

    assert_eq!(&result.parquet_bytes[0..4], b"PAR1");
    assert!(result.timestamp_nanos > 0);

    let batch = read_first_batch(&result.parquet_bytes);
    assert!(
        batch.num_rows() > 0,
        "JSONL payload should contribute at least one row"
    );
}

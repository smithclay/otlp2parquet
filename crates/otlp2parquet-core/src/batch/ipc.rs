//! Arrow IPC serialization for Durable Object storage.

use arrow::error::ArrowError;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use std::io::Cursor;

/// Minimum valid Arrow IPC stream size (continuation marker + metadata length).
const MIN_IPC_HEADER_SIZE: usize = 8;

/// Arrow IPC continuation marker (0xFFFFFFFF as little-endian i32 = -1).
const IPC_CONTINUATION_MARKER: i32 = -1;

/// Validate Arrow IPC stream header without full deserialization.
///
/// This performs a lightweight check to catch obviously malformed data
/// before attempting full deserialization, providing clearer error messages.
pub fn validate_ipc_header(data: &[u8]) -> Result<(), ArrowError> {
    // Arrow IPC streaming format starts with schema message
    // Minimum valid header: 4 bytes continuation + 4 bytes metadata length
    if data.len() < MIN_IPC_HEADER_SIZE {
        return Err(ArrowError::IpcError(format!(
            "IPC data too short: {} bytes (minimum {} required). \
             This may indicate truncated data or incompatible client.",
            data.len(),
            MIN_IPC_HEADER_SIZE
        )));
    }

    // Check for IPC continuation marker (0xFFFFFFFF) or legacy format
    // Modern Arrow IPC streams start with continuation marker
    let first_word = i32::from_le_bytes([data[0], data[1], data[2], data[3]]);

    // If it's the continuation marker, validate metadata length follows
    if first_word == IPC_CONTINUATION_MARKER {
        let metadata_len = i32::from_le_bytes([data[4], data[5], data[6], data[7]]);
        if metadata_len < 0 {
            return Err(ArrowError::IpcError(format!(
                "Invalid IPC metadata length: {}. Stream may be corrupted.",
                metadata_len
            )));
        }
    } else if first_word < 0 {
        // Negative value that isn't continuation marker is invalid
        return Err(ArrowError::IpcError(format!(
            "Invalid IPC header: unexpected value 0x{:08X}. \
             Expected continuation marker (0xFFFFFFFF) or positive metadata length.",
            first_word as u32
        )));
    }
    // Non-negative first word is legacy format (direct metadata length) - valid

    Ok(())
}

/// Serialize a RecordBatch to Arrow IPC stream format bytes.
pub fn serialize_batch(batch: &RecordBatch) -> Result<Vec<u8>, arrow::error::ArrowError> {
    let mut buffer = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buffer, batch.schema().as_ref())?;
        writer.write(batch)?;
        writer.finish()?;
    }
    Ok(buffer)
}

/// Deserialize Arrow IPC stream format bytes to a RecordBatch.
pub fn deserialize_batch(bytes: &[u8]) -> Result<RecordBatch, arrow::error::ArrowError> {
    let cursor = Cursor::new(bytes);
    let mut reader = StreamReader::try_new(cursor, None)?;
    reader
        .next()
        .ok_or_else(|| arrow::error::ArrowError::IpcError("No batch in stream".to_string()))?
}

/// Deserialize multiple IPC blobs without concatenating - returns iterator of batches.
///
/// This zero-copy approach allows writing each batch as a separate Parquet row group,
/// avoiding the full data copy that concat_batches performs.
pub fn deserialize_batches<'a, B>(
    blobs: &'a [B],
) -> impl Iterator<Item = Result<RecordBatch, arrow::error::ArrowError>> + 'a
where
    B: AsRef<[u8]> + 'a,
{
    blobs.iter().map(|b| deserialize_batch(b.as_ref()))
}

/// Deserialize multiple IPC blobs and concatenate into a single RecordBatch.
///
/// **Note**: This function creates a full copy of all data. For zero-copy operation,
/// use `deserialize_batches` and write each batch as a separate Parquet row group.
pub fn deserialize_and_concat(blobs: &[Vec<u8>]) -> Result<RecordBatch, arrow::error::ArrowError> {
    if blobs.is_empty() {
        return Err(arrow::error::ArrowError::InvalidArgumentError(
            "No blobs to concatenate".to_string(),
        ));
    }

    let batches: Result<Vec<RecordBatch>, _> = blobs.iter().map(|b| deserialize_batch(b)).collect();
    let batches = batches?;

    arrow::compute::concat_batches(&batches[0].schema(), &batches)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_validate_ipc_header_empty_data() {
        let result = validate_ipc_header(&[]);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("too short"));
    }

    #[test]
    fn test_validate_ipc_header_truncated_data() {
        let result = validate_ipc_header(&[0xFF, 0xFF, 0xFF, 0xFF]); // Only 4 bytes
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("too short"));
    }

    #[test]
    fn test_validate_ipc_header_invalid_continuation() {
        // Negative value that isn't -1 (continuation marker)
        let data = [0xFE, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00];
        let result = validate_ipc_header(&data);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Invalid IPC header"));
    }

    #[test]
    fn test_validate_ipc_header_valid_continuation_marker() {
        // Valid continuation marker followed by positive metadata length
        let data = [0xFF, 0xFF, 0xFF, 0xFF, 0x10, 0x00, 0x00, 0x00];
        let result = validate_ipc_header(&data);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_ipc_header_valid_legacy_format() {
        // Legacy format: direct positive metadata length (no continuation marker)
        let data = [0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        let result = validate_ipc_header(&data);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_ipc_header_real_serialized_batch() {
        // Validate that our serialized batches pass validation
        let batch = create_test_batch();
        let bytes = serialize_batch(&batch).unwrap();
        let result = validate_ipc_header(&bytes);
        assert!(result.is_ok(), "Serialized batch should pass validation");
    }

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let id_array = Int64Array::from(vec![1, 2, 3]);
        let name_array = StringArray::from(vec![Some("alice"), Some("bob"), None]);

        RecordBatch::try_new(schema, vec![Arc::new(id_array), Arc::new(name_array)]).unwrap()
    }

    #[test]
    fn test_serialize_deserialize_roundtrip() {
        let original = create_test_batch();
        let bytes = serialize_batch(&original).unwrap();
        let restored = deserialize_batch(&bytes).unwrap();

        assert_eq!(original.num_rows(), restored.num_rows());
        assert_eq!(original.num_columns(), restored.num_columns());
        assert_eq!(original.schema(), restored.schema());
    }

    #[test]
    fn test_deserialize_and_concat() {
        let batch1 = create_test_batch();
        let batch2 = create_test_batch();

        let bytes1 = serialize_batch(&batch1).unwrap();
        let bytes2 = serialize_batch(&batch2).unwrap();

        let combined = deserialize_and_concat(&[bytes1, bytes2]).unwrap();

        assert_eq!(combined.num_rows(), 6); // 3 + 3
        assert_eq!(combined.num_columns(), 2);
    }

    #[test]
    fn test_deserialize_empty_blobs_fails() {
        let result = deserialize_and_concat(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_deserialize_batches_round_trip_multiple() {
        let batch1 = create_test_batch();
        let batch2 = create_test_batch();

        let bytes1 = serialize_batch(&batch1).unwrap();
        let bytes2 = serialize_batch(&batch2).unwrap();

        let batches: Vec<_> = deserialize_batches(&[bytes1, bytes2])
            .collect::<Result<_, _>>()
            .unwrap();

        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].num_rows(), batch1.num_rows());
        assert_eq!(batches[1].num_rows(), batch2.num_rows());
        assert_eq!(batches[0].schema(), batches[1].schema());
    }
}

// Helper functions for creating Arrow fields with Parquet field IDs
//
// Field IDs provide stable field identification across schema evolution.
// When columns are added or removed, existing field IDs remain unchanged,
// ensuring compatibility with existing Parquet files.

use arrow::datatypes::{DataType, Field};
use std::collections::HashMap;

/// Creates a field with a Parquet field ID.
///
/// The `id` parameter is a mutable reference that is incremented after use,
/// allowing sequential ID assignment in schema definitions.
///
/// # Example
/// ```ignore
/// let mut id = 1;
/// let field1 = field_with_id("name", DataType::Utf8, false, &mut id);
/// let field2 = field_with_id("age", DataType::Int32, false, &mut id);
/// // field1 has ID 1, field2 has ID 2, id is now 3
/// ```
pub fn field_with_id(name: &str, data_type: DataType, nullable: bool, id: &mut i32) -> Field {
    let field = Field::new(name, data_type, nullable);
    let metadata = HashMap::from([("PARQUET:field_id".to_string(), id.to_string())]);
    *id += 1;
    field.with_metadata(metadata)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_field_with_id() {
        let mut id = 1;
        let field = field_with_id("test", DataType::Utf8, false, &mut id);

        assert_eq!(field.name(), "test");
        assert_eq!(field.data_type(), &DataType::Utf8);
        assert!(!field.is_nullable());
        assert_eq!(
            field.metadata().get("PARQUET:field_id"),
            Some(&"1".to_string())
        );
        assert_eq!(id, 2);
    }

    #[test]
    fn test_sequential_ids() {
        let mut id = 1;
        let field1 = field_with_id("field1", DataType::Utf8, false, &mut id);
        let field2 = field_with_id("field2", DataType::Int32, true, &mut id);
        let field3 = field_with_id("field3", DataType::Float64, false, &mut id);

        assert_eq!(
            field1.metadata().get("PARQUET:field_id"),
            Some(&"1".to_string())
        );
        assert_eq!(
            field2.metadata().get("PARQUET:field_id"),
            Some(&"2".to_string())
        );
        assert_eq!(
            field3.metadata().get("PARQUET:field_id"),
            Some(&"3".to_string())
        );
        assert_eq!(id, 4);
    }
}

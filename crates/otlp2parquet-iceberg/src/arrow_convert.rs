//! Arrow to Iceberg schema conversion (minimal implementation)
//!
//! Converts Arrow schemas to our minimal Iceberg schema types for REST API compatibility.

use crate::types::{NestedField, Schema, Type};
use anyhow::{anyhow, Context, Result};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};

/// Convert Arrow schema to Iceberg schema
///
/// Reads field IDs from Arrow field metadata (PARQUET:field_id) and creates
/// an Iceberg schema with matching field IDs.
pub fn arrow_to_iceberg_schema(arrow_schema: &ArrowSchema) -> Result<Schema> {
    let mut fields = Vec::new();

    for arrow_field in arrow_schema.fields() {
        let iceberg_field = convert_field(arrow_field)?;
        fields.push(iceberg_field);
    }

    Ok(Schema {
        schema_id: 0, // Default schema ID for new schemas
        fields,
        identifier_field_ids: None,
    })
}

/// Convert a single Arrow field to an Iceberg NestedField
fn convert_field(field: &Field) -> Result<NestedField> {
    // Extract field ID from metadata
    let field_id = field
        .metadata()
        .get("PARQUET:field_id")
        .ok_or_else(|| {
            anyhow!(
                "Field '{}' is missing PARQUET:field_id metadata",
                field.name()
            )
        })?
        .parse::<i32>()
        .with_context(|| {
            format!(
                "Failed to parse PARQUET:field_id for field '{}'",
                field.name()
            )
        })?;

    // Convert Arrow DataType to Iceberg Type
    let iceberg_type = convert_data_type(field.data_type(), field.name())?;

    // Create NestedField with appropriate nullability
    Ok(NestedField {
        id: field_id,
        name: field.name().to_string(),
        required: !field.is_nullable(),
        field_type: iceberg_type,
        doc: None,
    })
}

/// Convert Arrow DataType to Iceberg Type
///
/// # Unsigned Integer Handling
///
/// Iceberg does not have native unsigned integer types:
/// - `UInt32` → `Long` (64-bit signed safely represents all 32-bit unsigned)
/// - `UInt64` → **Error** (cannot be safely represented)
fn convert_data_type(data_type: &DataType, field_name: &str) -> Result<Type> {
    let iceberg_type = match data_type {
        DataType::Boolean => Type::Boolean,
        DataType::Int32 => Type::Int,
        DataType::Int64 => Type::Long,
        // Safe: UInt32 max (4,294,967,295) fits in Long
        DataType::UInt32 => Type::Long,
        // Unsafe: UInt64 values >= 2^63 would be misrepresented
        DataType::UInt64 => {
            return Err(anyhow!(
                "Field '{}' has type UInt64 which cannot be safely represented in Iceberg. \
                 Values >= 2^63 would appear as negative numbers.",
                field_name
            ))
        }
        DataType::Float32 => Type::Float,
        DataType::Float64 => Type::Double,
        DataType::Utf8 | DataType::LargeUtf8 => Type::String,
        DataType::Binary | DataType::LargeBinary => Type::Binary,
        DataType::FixedSizeBinary(len) => Type::Fixed {
            length: *len as u32,
        },
        DataType::Timestamp(TimeUnit::Microsecond, _) => Type::Timestamp,
        DataType::Timestamp(TimeUnit::Nanosecond, _) => Type::Timestamp, // Map to Timestamp for now
        DataType::Date32 => Type::Date,
        DataType::Time64(TimeUnit::Microsecond) => Type::Time,
        DataType::Decimal128(precision, scale) => Type::Decimal {
            precision: *precision as u32,
            scale: *scale as u32,
        },
        // Complex types - map to string for MVP
        DataType::Struct(_) | DataType::List(_) | DataType::Map(_, _) => Type::String,
        _ => {
            return Err(anyhow!(
                "Unsupported Arrow type for field '{}': {:?}",
                field_name,
                data_type
            ));
        }
    };

    Ok(iceberg_type)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn field_with_id(name: &str, data_type: DataType, nullable: bool, id: i32) -> Field {
        let metadata = HashMap::from([("PARQUET:field_id".to_string(), id.to_string())]);
        Field::new(name, data_type, nullable).with_metadata(metadata)
    }

    #[test]
    fn test_arrow_to_iceberg_basic_types() {
        let arrow_schema = ArrowSchema::new(vec![
            field_with_id("id", DataType::Int64, false, 1),
            field_with_id("name", DataType::Utf8, false, 2),
            field_with_id("active", DataType::Boolean, true, 3),
        ]);

        let iceberg_schema = arrow_to_iceberg_schema(&arrow_schema).unwrap();

        assert_eq!(iceberg_schema.fields.len(), 3);

        let id_field = iceberg_schema.field_by_name("id").unwrap();
        assert_eq!(id_field.id, 1);
        assert_eq!(id_field.field_type, Type::Long);
        assert!(id_field.required);

        let name_field = iceberg_schema.field_by_name("name").unwrap();
        assert_eq!(name_field.id, 2);
        assert_eq!(name_field.field_type, Type::String);
        assert!(name_field.required);

        let active_field = iceberg_schema.field_by_name("active").unwrap();
        assert_eq!(active_field.id, 3);
        assert_eq!(active_field.field_type, Type::Boolean);
        assert!(!active_field.required); // Optional = not required
    }

    #[test]
    fn test_field_id_stability() {
        let arrow_schema = ArrowSchema::new(vec![
            field_with_id("a", DataType::Int32, false, 10),
            field_with_id("b", DataType::Utf8, false, 20),
            field_with_id("c", DataType::Boolean, false, 30),
        ]);

        let iceberg_schema = arrow_to_iceberg_schema(&arrow_schema).unwrap();

        assert_eq!(iceberg_schema.field_by_name("a").unwrap().id, 10);
        assert_eq!(iceberg_schema.field_by_name("b").unwrap().id, 20);
        assert_eq!(iceberg_schema.field_by_name("c").unwrap().id, 30);
    }

    #[test]
    fn test_timestamp_conversion() {
        let arrow_schema = ArrowSchema::new(vec![field_with_id(
            "timestamp_us",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
            1,
        )]);

        let iceberg_schema = arrow_to_iceberg_schema(&arrow_schema).unwrap();

        assert_eq!(
            iceberg_schema
                .field_by_name("timestamp_us")
                .unwrap()
                .field_type,
            Type::Timestamp
        );
    }

    #[test]
    fn test_missing_field_id() {
        let arrow_schema = ArrowSchema::new(vec![Field::new("name", DataType::Utf8, false)]);

        let result = arrow_to_iceberg_schema(&arrow_schema);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("missing PARQUET:field_id"));
    }

    #[test]
    fn test_fixed_size_binary() {
        let arrow_schema = ArrowSchema::new(vec![
            field_with_id("trace_id", DataType::FixedSizeBinary(16), false, 1),
            field_with_id("span_id", DataType::FixedSizeBinary(8), false, 2),
        ]);

        let iceberg_schema = arrow_to_iceberg_schema(&arrow_schema).unwrap();

        assert_eq!(
            iceberg_schema.field_by_name("trace_id").unwrap().field_type,
            Type::Fixed { length: 16 }
        );
        assert_eq!(
            iceberg_schema.field_by_name("span_id").unwrap().field_type,
            Type::Fixed { length: 8 }
        );
    }

    #[test]
    fn test_uint64_error() {
        let arrow_schema =
            ArrowSchema::new(vec![field_with_id("count", DataType::UInt64, false, 1)]);

        let result = arrow_to_iceberg_schema(&arrow_schema);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("UInt64"));
    }
}

//! Arrow to Iceberg schema conversion.
//!
//! This module converts Arrow schemas to Iceberg schemas while preserving field IDs
//! from Arrow metadata (`PARQUET:field_id`).
//!
//! Field IDs provide stable field identification across schema evolution and must
//! match between the Parquet file metadata and the Iceberg table schema.
//!
//! # Field ID Preservation
//!
//! When Parquet files are written, each Arrow field has a `PARQUET:field_id` metadata
//! entry. This module reads those IDs and creates an Iceberg schema with matching
//! field IDs, ensuring:
//!
//! - Schema evolution can add/remove fields without breaking existing queries
//! - Column pruning works correctly across Parquet files and Iceberg metadata
//! - Statistics and data types remain consistent
//!
//! # Example
//!
//! ```no_run
//! use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
//! use std::collections::HashMap;
//! use otlp2parquet_iceberg::schema::arrow_to_iceberg_schema;
//!
//! # fn example() -> anyhow::Result<()> {
//! // Create Arrow field with field ID in metadata
//! let metadata = HashMap::from([("PARQUET:field_id".to_string(), "1".to_string())]);
//! let field = Field::new("timestamp", DataType::Int64, false).with_metadata(metadata);
//! let arrow_schema = ArrowSchema::new(vec![field]);
//!
//! // Convert to Iceberg schema (preserves field ID = 1)
//! let iceberg_schema = arrow_to_iceberg_schema(&arrow_schema, "logs")?;
//! # Ok(())
//! # }
//! ```

use anyhow::{anyhow, Context, Result};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use iceberg::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Type};

/// Convert Arrow schema to Iceberg schema.
///
/// Reads field IDs from Arrow field metadata (PARQUET:field_id) and creates
/// an Iceberg schema with matching field IDs. This ensures consistency between
/// Parquet file metadata and Iceberg table schema.
///
/// # Arguments
/// * `arrow_schema` - Arrow schema with PARQUET:field_id metadata on fields
/// * `_signal_type` - Signal type (logs, metrics, traces) for future use
///
/// # Returns
/// Iceberg schema with field IDs matching the Arrow schema
pub fn arrow_to_iceberg_schema(
    arrow_schema: &ArrowSchema,
    _signal_type: &str,
) -> Result<IcebergSchema> {
    use std::sync::Arc;

    let mut fields = Vec::new();

    for arrow_field in arrow_schema.fields() {
        let iceberg_field = convert_field(arrow_field)?;
        fields.push(Arc::new(iceberg_field));
    }

    IcebergSchema::builder()
        .with_fields(fields)
        .build()
        .context("failed to build Iceberg schema")
}

/// Convert a single Arrow field to an Iceberg NestedField.
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
    let nested_field = if field.is_nullable() {
        NestedField::optional(field_id, field.name(), iceberg_type)
    } else {
        NestedField::required(field_id, field.name(), iceberg_type)
    };

    Ok(nested_field)
}

/// Convert Arrow DataType to Iceberg Type.
///
/// # Unsigned Integer Handling
///
/// Iceberg does not have native unsigned integer types. We handle unsigned integers as follows:
/// - `UInt8/UInt16` → Not expected in OTLP schema (error if encountered)
/// - `UInt32` → `Long` (64-bit signed can safely represent all 32-bit unsigned values)
/// - `UInt64` → **Error** - Cannot be safely represented in Iceberg
///
/// OTLP spec uses unsigned integers for some fields (e.g., InstrumentationScope.dropped_attributes_count).
/// These are mapped to Long in our Arrow schema, so UInt types should be rare in practice.
fn convert_data_type(data_type: &DataType, field_name: &str) -> Result<Type> {
    let iceberg_type = match data_type {
        DataType::Boolean => Type::Primitive(PrimitiveType::Boolean),
        DataType::Int32 => Type::Primitive(PrimitiveType::Int),
        DataType::Int64 => Type::Primitive(PrimitiveType::Long),
        // Safe: UInt32 max (4,294,967,295) fits in Long (9,223,372,036,854,775,807)
        DataType::UInt32 => Type::Primitive(PrimitiveType::Long),
        // Unsafe: UInt64 values >= 2^63 would be misrepresented as negative in Iceberg
        DataType::UInt64 => {
            return Err(anyhow!(
                "Field '{}' has type UInt64 which cannot be safely represented in Iceberg. \
                 Values >= 2^63 would appear as negative numbers. \
                 Consider using Int64 in the Arrow schema if the data range allows.",
                field_name
            ))
        }
        DataType::Float32 => Type::Primitive(PrimitiveType::Float),
        DataType::Float64 => Type::Primitive(PrimitiveType::Double),
        DataType::Utf8 | DataType::LargeUtf8 => Type::Primitive(PrimitiveType::String),
        DataType::Binary | DataType::LargeBinary => Type::Primitive(PrimitiveType::Binary),
        DataType::FixedSizeBinary(len) => Type::Primitive(PrimitiveType::Fixed(*len as u64)),
        DataType::Timestamp(TimeUnit::Microsecond, _) => Type::Primitive(PrimitiveType::Timestamp),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => Type::Primitive(PrimitiveType::TimestampNs),
        DataType::Date32 => Type::Primitive(PrimitiveType::Date),
        DataType::Time64(TimeUnit::Microsecond) => Type::Primitive(PrimitiveType::Time),
        DataType::Decimal128(precision, scale) => Type::Primitive(PrimitiveType::Decimal {
            precision: *precision as u32,
            scale: *scale as u32,
        }),
        // For complex types, we'll use a simplified mapping
        // TODO: Implement full nested type conversion when needed
        DataType::Struct(_) | DataType::List(_) | DataType::Map(_, _) => {
            // For now, map complex types to string for MVP
            // This allows the schema to be created but queries may need special handling
            Type::Primitive(PrimitiveType::String)
        }
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
    use arrow::datatypes::Field;
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

        let iceberg_schema = arrow_to_iceberg_schema(&arrow_schema, "test").unwrap();

        assert_eq!(iceberg_schema.as_struct().fields().len(), 3);

        let id_field = iceberg_schema.field_by_name("id").unwrap();
        assert_eq!(id_field.id, 1);
        assert_eq!(*id_field.field_type, Type::Primitive(PrimitiveType::Long));
        assert!(id_field.required);

        let name_field = iceberg_schema.field_by_name("name").unwrap();
        assert_eq!(name_field.id, 2);
        assert_eq!(
            *name_field.field_type,
            Type::Primitive(PrimitiveType::String)
        );
        assert!(name_field.required);

        let active_field = iceberg_schema.field_by_name("active").unwrap();
        assert_eq!(active_field.id, 3);
        assert_eq!(
            *active_field.field_type,
            Type::Primitive(PrimitiveType::Boolean)
        );
        assert!(!active_field.required); // Optional = not required
    }

    #[test]
    fn test_field_id_stability() {
        // Field IDs should match exactly what's in metadata
        let arrow_schema = ArrowSchema::new(vec![
            field_with_id("a", DataType::Int32, false, 10),
            field_with_id("b", DataType::Utf8, false, 20),
            field_with_id("c", DataType::Boolean, false, 30),
        ]);

        let iceberg_schema = arrow_to_iceberg_schema(&arrow_schema, "test").unwrap();

        assert_eq!(iceberg_schema.field_by_name("a").unwrap().id, 10);
        assert_eq!(iceberg_schema.field_by_name("b").unwrap().id, 20);
        assert_eq!(iceberg_schema.field_by_name("c").unwrap().id, 30);
    }

    #[test]
    fn test_timestamp_conversion() {
        let arrow_schema = ArrowSchema::new(vec![
            field_with_id(
                "timestamp_us",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
                1,
            ),
            field_with_id(
                "timestamp_ns",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
                2,
            ),
        ]);

        let iceberg_schema = arrow_to_iceberg_schema(&arrow_schema, "test").unwrap();

        assert_eq!(
            *iceberg_schema
                .field_by_name("timestamp_us")
                .unwrap()
                .field_type,
            Type::Primitive(PrimitiveType::Timestamp)
        );
        assert_eq!(
            *iceberg_schema
                .field_by_name("timestamp_ns")
                .unwrap()
                .field_type,
            Type::Primitive(PrimitiveType::TimestampNs)
        );
    }

    #[test]
    fn test_missing_field_id() {
        // Field without PARQUET:field_id should error
        let arrow_schema = ArrowSchema::new(vec![Field::new("name", DataType::Utf8, false)]);

        let result = arrow_to_iceberg_schema(&arrow_schema, "test");
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

        let iceberg_schema = arrow_to_iceberg_schema(&arrow_schema, "test").unwrap();

        assert_eq!(
            *iceberg_schema.field_by_name("trace_id").unwrap().field_type,
            Type::Primitive(PrimitiveType::Fixed(16))
        );
        assert_eq!(
            *iceberg_schema.field_by_name("span_id").unwrap().field_type,
            Type::Primitive(PrimitiveType::Fixed(8))
        );
    }
}

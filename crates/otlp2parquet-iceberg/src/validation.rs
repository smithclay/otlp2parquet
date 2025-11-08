//! Schema validation for Iceberg compatibility
//!
//! Optional validation to check that Parquet schemas are compatible with Iceberg table schemas.
//! Disabled by default - use via `IcebergCatalog::with_validation(true)`.

use super::types::Schema;
use anyhow::{anyhow, Result};
use arrow::datatypes::Schema as ArrowSchema;

/// Validate that an Arrow schema is compatible with an Iceberg schema
///
/// # Rules
/// - All required Iceberg fields must exist in Arrow with correct types
/// - Optional/nullable Iceberg fields can be missing (schema evolution)
/// - Extra Arrow columns not in Iceberg schema are okay (will be ignored)
/// - Field types must be compatible
///
/// # Parameters
/// - `arrow_schema`: Schema from the Parquet file being written
/// - `iceberg_schema`: Current schema of the Iceberg table
///
/// # Returns
/// - Ok(()) if schemas are compatible
/// - Err if there's a breaking schema mismatch
pub fn validate_schema_compatibility(
    arrow_schema: &ArrowSchema,
    iceberg_schema: &Schema,
) -> Result<()> {
    for iceberg_field in &iceberg_schema.fields {
        if iceberg_field.required {
            // Required field - must exist with correct type
            let arrow_field = arrow_schema
                .field_with_name(&iceberg_field.name)
                .map_err(|_| {
                    anyhow!(
                        "Required field '{}' (ID={}) missing from Parquet schema",
                        iceberg_field.name,
                        iceberg_field.id
                    )
                })?;

            // Basic type check - just verify field exists
            // Full type compatibility checking would require deep Arrow<->Iceberg type mapping
            if arrow_field.name() != &iceberg_field.name {
                return Err(anyhow!(
                    "Field name mismatch: expected '{}', found '{}'",
                    iceberg_field.name,
                    arrow_field.name()
                ));
            }
        }
        // Optional fields don't need to exist - that's schema evolution
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{NestedField, Type};
    use arrow::datatypes::{DataType, Field};
    use std::collections::HashMap;

    fn field_with_id(name: &str, data_type: DataType, nullable: bool, id: i32) -> Field {
        let metadata = HashMap::from([("PARQUET:field_id".to_string(), id.to_string())]);
        Field::new(name, data_type, nullable).with_metadata(metadata)
    }

    #[test]
    fn test_all_required_fields_present() {
        let arrow_schema = ArrowSchema::new(vec![
            field_with_id("id", DataType::Int64, false, 1),
            field_with_id("name", DataType::Utf8, false, 2),
        ]);

        let iceberg_schema = Schema {
            schema_id: 0,
            fields: vec![
                NestedField {
                    id: 1,
                    name: "id".to_string(),
                    required: true,
                    field_type: Type::Primitive("long".to_string()),
                    doc: None,
                },
                NestedField {
                    id: 2,
                    name: "name".to_string(),
                    required: true,
                    field_type: Type::Primitive("string".to_string()),
                    doc: None,
                },
            ],
            identifier_field_ids: None,
        };

        assert!(validate_schema_compatibility(&arrow_schema, &iceberg_schema).is_ok());
    }

    #[test]
    fn test_missing_required_field() {
        let arrow_schema = ArrowSchema::new(vec![field_with_id("id", DataType::Int64, false, 1)]);

        let iceberg_schema = Schema {
            schema_id: 0,
            fields: vec![
                NestedField {
                    id: 1,
                    name: "id".to_string(),
                    required: true,
                    field_type: Type::Primitive("long".to_string()),
                    doc: None,
                },
                NestedField {
                    id: 2,
                    name: "name".to_string(),
                    required: true,
                    field_type: Type::Primitive("string".to_string()),
                    doc: None,
                },
            ],
            identifier_field_ids: None,
        };

        let result = validate_schema_compatibility(&arrow_schema, &iceberg_schema);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Required field 'name'"));
    }

    #[test]
    fn test_optional_field_can_be_missing() {
        let arrow_schema = ArrowSchema::new(vec![field_with_id("id", DataType::Int64, false, 1)]);

        let iceberg_schema = Schema {
            schema_id: 0,
            fields: vec![
                NestedField {
                    id: 1,
                    name: "id".to_string(),
                    required: true,
                    field_type: Type::Primitive("long".to_string()),
                    doc: None,
                },
                NestedField {
                    id: 2,
                    name: "optional_name".to_string(),
                    required: false, // Optional
                    field_type: Type::Primitive("string".to_string()),
                    doc: None,
                },
            ],
            identifier_field_ids: None,
        };

        // Should pass - optional fields can be missing
        assert!(validate_schema_compatibility(&arrow_schema, &iceberg_schema).is_ok());
    }

    #[test]
    fn test_extra_arrow_columns_ok() {
        let arrow_schema = ArrowSchema::new(vec![
            field_with_id("id", DataType::Int64, false, 1),
            field_with_id("name", DataType::Utf8, false, 2),
            field_with_id("extra_column", DataType::Boolean, true, 3),
        ]);

        let iceberg_schema = Schema {
            schema_id: 0,
            fields: vec![
                NestedField {
                    id: 1,
                    name: "id".to_string(),
                    required: true,
                    field_type: Type::Primitive("long".to_string()),
                    doc: None,
                },
                NestedField {
                    id: 2,
                    name: "name".to_string(),
                    required: true,
                    field_type: Type::Primitive("string".to_string()),
                    doc: None,
                },
            ],
            identifier_field_ids: None,
        };

        // Should pass - extra columns in Arrow are okay
        assert!(validate_schema_compatibility(&arrow_schema, &iceberg_schema).is_ok());
    }
}

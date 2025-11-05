//! Iceberg schema types
//!
//! Minimal implementation of Iceberg schema types for REST API compatibility.
//! Based on Iceberg Table Spec v2: https://iceberg.apache.org/spec/#schemas

use serde::{Deserialize, Serialize};

/// Iceberg schema definition
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Schema {
    /// Unique schema identifier
    pub schema_id: i32,
    /// Top-level fields in the schema
    pub fields: Vec<NestedField>,
    /// Optional identifier field IDs (for identifying records)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identifier_field_ids: Option<Vec<i32>>,
}

impl Schema {
    /// Find a field by name (top-level only)
    pub fn field_by_name(&self, name: &str) -> Option<&NestedField> {
        self.fields.iter().find(|f| f.name == name)
    }

    /// Find a field by ID (searches recursively)
    pub fn field_by_id(&self, id: i32) -> Option<&NestedField> {
        for field in &self.fields {
            if let Some(found) = field.find_by_id(id) {
                return Some(found);
            }
        }
        None
    }
}

/// Iceberg nested field definition
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct NestedField {
    /// Unique field identifier
    pub id: i32,
    /// Field name
    pub name: String,
    /// Whether field is required (non-nullable)
    pub required: bool,
    /// Field data type
    #[serde(rename = "type")]
    pub field_type: Type,
    /// Optional documentation string
    #[serde(skip_serializing_if = "Option::is_none")]
    pub doc: Option<String>,
}

impl NestedField {
    /// Recursively find a field by ID
    pub fn find_by_id(&self, id: i32) -> Option<&NestedField> {
        if self.id == id {
            return Some(self);
        }
        match &self.field_type {
            Type::Struct { fields } => {
                for field in fields {
                    if let Some(found) = field.find_by_id(id) {
                        return Some(found);
                    }
                }
                None
            }
            Type::List { element, .. } => element.find_by_id(id),
            Type::Map { key, value, .. } => key.find_by_id(id).or_else(|| value.find_by_id(id)),
            _ => None,
        }
    }
}

/// Iceberg data types
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase", tag = "type")]
pub enum Type {
    /// Boolean type
    Boolean,
    /// 32-bit signed integer
    Int,
    /// 64-bit signed integer
    Long,
    /// 32-bit IEEE 754 floating point
    Float,
    /// 64-bit IEEE 754 floating point
    Double,
    /// Decimal number with precision and scale
    Decimal { precision: u32, scale: u32 },
    /// Calendar date (days since epoch)
    Date,
    /// Time of day (microseconds since midnight)
    Time,
    /// Timestamp without timezone (microseconds since epoch)
    Timestamp,
    /// Timestamp with timezone (microseconds since epoch, stored as UTC)
    #[serde(rename = "timestamptz")]
    TimestampTz,
    /// UTF-8 encoded string
    String,
    /// UUID
    Uuid,
    /// Fixed-length byte array
    Fixed { length: u32 },
    /// Variable-length byte array
    Binary,
    /// Struct type (record with named fields)
    Struct { fields: Vec<NestedField> },
    /// List type (array of elements)
    List {
        #[serde(rename = "element-id")]
        element_id: i32,
        element: Box<NestedField>,
        #[serde(rename = "element-required")]
        element_required: bool,
    },
    /// Map type (key-value pairs)
    Map {
        #[serde(rename = "key-id")]
        key_id: i32,
        key: Box<NestedField>,
        #[serde(rename = "value-id")]
        value_id: i32,
        value: Box<NestedField>,
        #[serde(rename = "value-required")]
        value_required: bool,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_primitive_type_serialization() {
        let type_json = r#"{"type":"int"}"#;
        let typ: Type = serde_json::from_str(type_json).unwrap();
        assert_eq!(typ, Type::Int);

        let serialized = serde_json::to_string(&typ).unwrap();
        assert_eq!(serialized, type_json);
    }

    #[test]
    fn test_decimal_type_serialization() {
        let type_json = r#"{"type":"decimal","precision":10,"scale":2}"#;
        let typ: Type = serde_json::from_str(type_json).unwrap();
        assert_eq!(
            typ,
            Type::Decimal {
                precision: 10,
                scale: 2
            }
        );

        let serialized = serde_json::to_string(&typ).unwrap();
        assert_eq!(serialized, type_json);
    }

    #[test]
    fn test_struct_type_serialization() {
        let type_json = r#"{"type":"struct","fields":[{"id":1,"name":"field1","required":true,"type":{"type":"int"}}]}"#;
        let typ: Type = serde_json::from_str(type_json).unwrap();

        if let Type::Struct { fields } = typ {
            assert_eq!(fields.len(), 1);
            assert_eq!(fields[0].id, 1);
            assert_eq!(fields[0].name, "field1");
            assert!(fields[0].required);
            assert_eq!(fields[0].field_type, Type::Int);
        } else {
            panic!("Expected Struct type");
        }
    }

    #[test]
    fn test_schema_serialization() {
        let schema = Schema {
            schema_id: 0,
            fields: vec![
                NestedField {
                    id: 1,
                    name: "id".to_string(),
                    required: true,
                    field_type: Type::Long,
                    doc: None,
                },
                NestedField {
                    id: 2,
                    name: "name".to_string(),
                    required: false,
                    field_type: Type::String,
                    doc: Some("User name".to_string()),
                },
            ],
            identifier_field_ids: Some(vec![1]),
        };

        let json = serde_json::to_string(&schema).unwrap();
        let deserialized: Schema = serde_json::from_str(&json).unwrap();
        assert_eq!(schema, deserialized);
    }

    #[test]
    fn test_field_by_name() {
        let schema = Schema {
            schema_id: 0,
            fields: vec![NestedField {
                id: 1,
                name: "id".to_string(),
                required: true,
                field_type: Type::Long,
                doc: None,
            }],
            identifier_field_ids: None,
        };

        assert!(schema.field_by_name("id").is_some());
        assert!(schema.field_by_name("missing").is_none());
    }

    #[test]
    fn test_field_by_id() {
        let schema = Schema {
            schema_id: 0,
            fields: vec![NestedField {
                id: 1,
                name: "outer".to_string(),
                required: true,
                field_type: Type::Struct {
                    fields: vec![NestedField {
                        id: 2,
                        name: "inner".to_string(),
                        required: false,
                        field_type: Type::String,
                        doc: None,
                    }],
                },
                doc: None,
            }],
            identifier_field_ids: None,
        };

        // Find outer field
        assert_eq!(schema.field_by_id(1).unwrap().name, "outer");
        // Find nested field
        assert_eq!(schema.field_by_id(2).unwrap().name, "inner");
        // Missing field
        assert!(schema.field_by_id(999).is_none());
    }
}

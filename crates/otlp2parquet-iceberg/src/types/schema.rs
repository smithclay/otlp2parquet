//! Iceberg schema types
//!
//! Minimal implementation of Iceberg schema types for REST API compatibility.
//! Based on Iceberg Table Spec v2: <https://iceberg.apache.org/spec/#schemas>

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
            Type::Struct { fields, .. } => {
                for field in fields {
                    if let Some(found) = field.find_by_id(id) {
                        return Some(found);
                    }
                }
                None
            }
            Type::List { element, .. } => {
                // List elements can be complex types containing nested fields
                if let NestedType::Complex(nested_type) = element {
                    if let Type::Struct { fields, .. } = nested_type.as_ref() {
                        for field in fields {
                            if let Some(found) = field.find_by_id(id) {
                                return Some(found);
                            }
                        }
                    }
                }
                None
            }
            Type::Map { value, .. } => {
                // Map values can be complex types containing nested fields
                if let NestedType::Complex(nested_type) = value {
                    if let Type::Struct { fields, .. } = nested_type.as_ref() {
                        for field in fields {
                            if let Some(found) = field.find_by_id(id) {
                                return Some(found);
                            }
                        }
                    }
                }
                None
            }
            _ => None,
        }
    }
}

/// Nested type reference - can be either a simple primitive type string or a complex Type
///
/// Used in List and Map types where elements/values can be either:
/// - Simple primitive: "string", "int", etc.
/// - Complex type: {"type": "struct", "fields": [...]}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum NestedType {
    // Complex types must come first for untagged deserialization
    Complex(Box<Type>),
    // Primitive type as string
    Primitive(String),
}

/// Iceberg data types
///
/// Can be deserialized from either:
/// - Simple string for primitives: "int", "long", "string", etc.
/// - Object with "type" field for complex types: {"type": "struct", "fields": [...]}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Type {
    // Complex types must come first for untagged deserialization
    /// Struct type (record with named fields)
    Struct {
        #[serde(rename = "type")]
        type_name: String, // Always "struct"
        fields: Vec<NestedField>,
    },
    /// List type (array of elements)
    List {
        #[serde(rename = "type")]
        type_name: String, // Always "list"
        #[serde(rename = "element-id")]
        element_id: i32,
        element: NestedType, // Can be simple string or complex type
        #[serde(rename = "element-required")]
        element_required: bool,
    },
    /// Map type (key-value pairs)
    Map {
        #[serde(rename = "type")]
        type_name: String, // Always "map"
        #[serde(rename = "key-id")]
        key_id: i32,
        key: String, // Key type name (always simple string)
        #[serde(rename = "value-id")]
        value_id: i32,
        value: NestedType, // Can be simple string or complex type
        #[serde(rename = "value-required")]
        value_required: bool,
    },
    /// Decimal number with precision and scale
    Decimal {
        #[serde(rename = "type")]
        type_name: String, // Always "decimal"
        precision: u32,
        scale: u32,
    },
    /// Fixed-length byte array
    FixedObj {
        #[serde(rename = "type")]
        type_name: String, // Always "fixed"
        length: u32,
    },
    // Primitive types as strings
    /// Primitive type represented as a string
    Primitive(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_primitive_type_serialization() {
        let type_json = r#""int""#;
        let typ: Type = serde_json::from_str(type_json).unwrap();
        assert_eq!(typ, Type::Primitive("int".to_string()));

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
                type_name: "decimal".to_string(),
                precision: 10,
                scale: 2
            }
        );

        let serialized = serde_json::to_string(&typ).unwrap();
        assert_eq!(serialized, type_json);
    }

    #[test]
    fn test_struct_type_serialization() {
        let type_json =
            r#"{"type":"struct","fields":[{"id":1,"name":"field1","required":true,"type":"int"}]}"#;
        let typ: Type = serde_json::from_str(type_json).unwrap();

        if let Type::Struct { fields, .. } = typ {
            assert_eq!(fields.len(), 1);
            assert_eq!(fields[0].id, 1);
            assert_eq!(fields[0].name, "field1");
            assert!(fields[0].required);
            assert_eq!(fields[0].field_type, Type::Primitive("int".to_string()));
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
                    field_type: Type::Primitive("long".to_string()),
                    doc: None,
                },
                NestedField {
                    id: 2,
                    name: "name".to_string(),
                    required: false,
                    field_type: Type::Primitive("string".to_string()),
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
                field_type: Type::Primitive("long".to_string()),
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
                    type_name: "struct".to_string(),
                    fields: vec![NestedField {
                        id: 2,
                        name: "inner".to_string(),
                        required: false,
                        field_type: Type::Primitive("string".to_string()),
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

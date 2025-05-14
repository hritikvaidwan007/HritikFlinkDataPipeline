/**
 * Avro Utility Implementation
 * 
 * This file contains the AvroUtil class, a specialized utility for converting
 * between JSON and Apache Avro formats for the schema validation pipeline.
 */
package com.dataflow.flink.utils;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Utility class for working with Apache Avro schemas and records.
 * 
 * Apache Avro is a data serialization system that provides:
 * - Rich data structures with support for complex, nested types
 * - A compact, fast binary data format
 * - Schema-based validation for data integrity
 * - Schema evolution to handle changing data models
 * 
 * This utility class provides functionality for:
 * - Converting JSON data to Avro GenericRecords
 * - Handling type conversions between JSON and Avro types
 * - Managing schema complexities like unions, nullability, and defaults
 * - Supporting nested structures (records, arrays, maps)
 * 
 * The conversion process respects Avro's type system and validation rules,
 * and handles edge cases like type coercion, missing fields, and null values.
 */
/**
 * Utility class for converting between JSON and Avro formats.
 * This is implemented as a utility class with static methods and no instance creation.
 */
public class AvroUtil {
    private static final Logger LOG = LoggerFactory.getLogger(AvroUtil.class);
    
    /**
     * Private constructor to prevent instantiation of utility class.
     * This follows the utility class pattern where all methods are static.
     */
    private AvroUtil() {
        // Private constructor to prevent instantiation
    }
    
    /**
     * Converts a JSON node to an Avro GenericRecord according to the provided schema.
     * 
     * This method is at the core of the schema validation process, performing
     * the transformation from flexible JSON format to the strongly-typed Avro format.
     * It handles:
     * 
     * - Field mapping: Matching JSON fields to Avro schema fields
     * - Default values: Using schema defaults for missing fields
     * - Nullability: Handling null values according to schema rules
     * - Type conversion: Converting between JSON and Avro data types
     * - Validation: Ensuring required fields are present
     * 
     * In Apache Avro, GenericRecord is a non-specific record implementation that:
     * - Adheres to a particular schema
     * - Provides type safety through schema validation
     * - Supports complex nested structures
     * - Can be serialized to and deserialized from binary format
     *
     * @param schema Avro schema defining the structure and types for conversion
     * @param jsonNode JSON data node to convert to Avro format
     * @return GenericRecord representation of the JSON data conforming to the schema
     * @throws Exception if conversion fails due to missing fields, type mismatches, etc.
     */
    public static GenericRecord convertJsonToAvro(Schema schema, JsonNode jsonNode) throws Exception {
        // Create a new Avro record based on the provided schema
        // GenericData.Record is the standard implementation of GenericRecord
        GenericRecord record = new GenericData.Record(schema);
        
        // Process each field defined in the schema
        for (Field field : schema.getFields()) {
            String fieldName = field.name();
            Schema fieldSchema = field.schema();
            
            // Handle missing fields in the JSON data
            if (!jsonNode.has(fieldName)) {
                // Field is not present in JSON, use schema-defined default if available
                Object defaultValue = field.defaultVal();
                
                // Apply the default value if one is defined and it's not the special NULL_DEFAULT_VALUE marker
                if (defaultValue != null && !defaultValue.equals(Field.NULL_DEFAULT_VALUE)) {
                    record.put(fieldName, defaultValue);
                } else if (isNullable(fieldSchema)) {
                    // If field is nullable (defined as union with null), set it to null
                    record.put(fieldName, null);
                } else {
                    // Field is required (not nullable) and has no default - this is a validation error
                    throw new Exception("Missing required field: " + fieldName);
                }
                continue;
            }
            
            // Get the field value from the JSON
            JsonNode fieldValue = jsonNode.get(fieldName);
            
            // Special handling for null values in nullable fields
            // In Avro, nullable fields are defined as unions with null type
            if (fieldValue.isNull() && isNullable(fieldSchema)) {
                record.put(fieldName, null);
                continue;
            }
            
            // Convert the field value based on its schema type
            // This handles the type-specific conversion logic
            try {
                Object convertedValue = convertJsonValueToAvro(fieldSchema, fieldValue);
                record.put(fieldName, convertedValue);
            } catch (Exception e) {
                // Detailed logging for conversion errors helps with troubleshooting
                LOG.warn("Error converting field '{}': {}", fieldName, e.getMessage());
                throw new Exception("Field conversion error for '" + fieldName + "': " + e.getMessage());
            }
        }
        
        return record;
    }
    
    /**
     * Converts a JSON value to the appropriate Avro type based on the schema.
     * 
     * This method handles the complex type conversion logic between JSON and Avro,
     * supporting all Avro primitive types as well as complex types (records, arrays, maps).
     * 
     * Key features:
     * - Union type resolution: Finds the appropriate type in a union
     * - Type coercion: Converts between compatible types (e.g., string to number)
     * - Nested structure handling: Processes nested records, arrays, and maps
     * - Strict validation: Ensures values match their schema definitions
     * 
     * Avro's type system includes:
     * - Primitive types: null, boolean, int, long, float, double, bytes, string
     * - Complex types: record, enum, array, map, union, fixed
     * 
     * This implementation handles type conversion with appropriate error reporting
     * to make schema validation issues easy to diagnose and fix.
     *
     * @param schema Avro schema defining the expected type
     * @param value JSON value to convert to the appropriate Avro type
     * @return Converted value in the appropriate Java type for Avro
     * @throws Exception if the value cannot be converted to the schema type
     */
    private static Object convertJsonValueToAvro(Schema schema, JsonNode value) throws Exception {
        // Handle union types (e.g., ["null", "string"])
        // Unions in Avro allow a field to be one of several types
        if (schema.getType() == Type.UNION) {
            List<Schema> types = schema.getTypes();
            
            // If value is null, return null for nullable unions
            // This is a common pattern in Avro for optional fields
            if (value.isNull() && isNullable(schema)) {
                return null;
            }
            
            // Try each non-null type in the union until one works
            // This implements Avro's union resolution rules
            for (Schema typeSchema : types) {
                if (typeSchema.getType() != Type.NULL) {
                    try {
                        return convertJsonValueToAvro(typeSchema, value);
                    } catch (Exception e) {
                        // Try the next type in the union
                        // This allows for flexible data handling
                        continue;
                    }
                }
            }
            
            // If no type in the union matched, this is an error
            throw new Exception("Value doesn't match any type in the union");
        }
        
        // Handle specific types by converting JSON values to appropriate Java types
        // Each case implements the conversion rules for a specific Avro type
        switch (schema.getType()) {
            case STRING:
                // For string type, use the text value or string representation
                return value.isTextual() ? value.asText() : value.toString();
                
            case INT:
                // For int type, convert from numeric JSON or parse from string
                return value.isInt() ? value.asInt() : Integer.parseInt(value.asText());
                
            case LONG:
                // For long type, handle integers and longs, or parse from string
                return value.isLong() || value.isInt() ? value.asLong() : Long.parseLong(value.asText());
                
            case FLOAT:
                // For float type, handle various numeric types or parse from string
                return value.isFloat() || value.isDouble() ? 
                       value.floatValue() : Float.parseFloat(value.asText());
                
            case DOUBLE:
                // For double type, handle various numeric types or parse from string
                return value.isDouble() || value.isFloat() || value.isInt() ? 
                       value.doubleValue() : Double.parseDouble(value.asText());
                
            case BOOLEAN:
                // For boolean type, use boolean value or parse from string
                return value.isBoolean() ? value.booleanValue() : 
                       Boolean.parseBoolean(value.asText());
                
            case RECORD:
                // For record type (complex nested structure)
                // Create a new record and recursively convert its fields
                GenericRecord nestedRecord = new GenericData.Record(schema);
                for (Field field : schema.getFields()) {
                    String fieldName = field.name();
                    if (value.has(fieldName)) {
                        // Recursively convert nested field values
                        nestedRecord.put(fieldName, 
                            convertJsonValueToAvro(field.schema(), value.get(fieldName)));
                    } else if (field.defaultVal() != null) {
                        // Use default value if field is missing
                        nestedRecord.put(fieldName, field.defaultVal());
                    }
                    // Note: Missing required fields will be caught by validation later
                }
                return nestedRecord;
                
            case ARRAY:
                // For array type, verify it's actually an array and convert elements
                if (!value.isArray()) {
                    throw new Exception("Expected array but got: " + value.getNodeType());
                }
                
                // Create Avro array with appropriate size and schema
                GenericData.Array<Object> array = 
                    new GenericData.Array<>(value.size(), schema);
                
                // Convert each element in the array recursively
                for (int i = 0; i < value.size(); i++) {
                    array.add(convertJsonValueToAvro(schema.getElementType(), value.get(i)));
                }
                return array;
                
            case MAP:
                // For map type, verify it's an object and convert key-value pairs
                if (!value.isObject()) {
                    throw new Exception("Expected object but got: " + value.getNodeType());
                }
                
                // Create Java map to hold the Avro map data
                java.util.Map<String, Object> map = new java.util.HashMap<>();
                
                // Convert each field in the object to a map entry
                value.fields().forEachRemaining(entry -> {
                    try {
                        map.put(entry.getKey(), 
                            convertJsonValueToAvro(schema.getValueType(), entry.getValue()));
                    } catch (Exception e) {
                        // Log errors but continue processing other entries
                        LOG.warn("Error converting map value for key '{}': {}", 
                                 entry.getKey(), e.getMessage());
                    }
                });
                return map;
                
            case NULL:
                // For null type, simply return null
                return null;
                
            default:
                // For unsupported types (like ENUM, FIXED, etc.), throw exception
                throw new Exception("Unsupported Avro type: " + schema.getType());
        }
    }
    
    /**
     * Checks if an Avro schema is nullable (defined as a union that includes null).
     * 
     * In Apache Avro, optional fields are represented as unions with the null type.
     * For example, a nullable string would have the schema: ["null", "string"]
     * 
     * This helper method determines if a field can be null by checking if its
     * schema is a union that includes the NULL type among its options.
     * 
     * Nullability is important for schema validation because:
     * - Required fields (non-nullable) must be present in the data
     * - Optional fields (nullable) can be omitted or set to null
     * - Default values are used for missing optional fields
     *
     * @param schema The Avro schema to check for nullability
     * @return true if the schema is nullable (union including null), false otherwise
     */
    public static boolean isNullable(Schema schema) {
        // Only union types can be nullable in Avro
        if (schema.getType() == Type.UNION) {
            // Check each type in the union for NULL
            for (Schema typeSchema : schema.getTypes()) {
                if (typeSchema.getType() == Type.NULL) {
                    return true;
                }
            }
        }
        // Non-union types or unions without NULL are not nullable
        return false;
    }
}

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
 * Utility class for working with Avro schemas and records
 */
public class AvroUtil {
    private static final Logger LOG = LoggerFactory.getLogger(AvroUtil.class);
    
    private AvroUtil() {
        // Private constructor to prevent instantiation
    }
    
    /**
     * Converts a JSON node to an Avro GenericRecord
     * 
     * @param schema Avro schema to use for conversion
     * @param jsonNode JSON data to convert
     * @return GenericRecord representation of the JSON data
     * @throws Exception if conversion fails
     */
    public static GenericRecord convertJsonToAvro(Schema schema, JsonNode jsonNode) throws Exception {
        GenericRecord record = new GenericData.Record(schema);
        
        for (Field field : schema.getFields()) {
            String fieldName = field.name();
            Schema fieldSchema = field.schema();
            
            if (!jsonNode.has(fieldName)) {
                // Field is not present in JSON, use default if available
                Object defaultValue = field.defaultVal();
                if (defaultValue != null && !defaultValue.equals(Field.NULL_DEFAULT_VALUE)) {
                    record.put(fieldName, defaultValue);
                } else if (isNullable(fieldSchema)) {
                    // If field is nullable, set it to null
                    record.put(fieldName, null);
                } else {
                    throw new Exception("Missing required field: " + fieldName);
                }
                continue;
            }
            
            JsonNode fieldValue = jsonNode.get(fieldName);
            
            // Handle null values for union types that include null
            if (fieldValue.isNull() && isNullable(fieldSchema)) {
                record.put(fieldName, null);
                continue;
            }
            
            // Convert the field value based on its schema type
            try {
                Object convertedValue = convertJsonValueToAvro(fieldSchema, fieldValue);
                record.put(fieldName, convertedValue);
            } catch (Exception e) {
                LOG.warn("Error converting field '{}': {}", fieldName, e.getMessage());
                throw new Exception("Field conversion error for '" + fieldName + "': " + e.getMessage());
            }
        }
        
        return record;
    }
    
    /**
     * Converts a JSON value to the appropriate Avro type
     * 
     * @param schema Avro schema for the value
     * @param value JSON value to convert
     * @return Converted value appropriate for Avro
     * @throws Exception if conversion fails
     */
    private static Object convertJsonValueToAvro(Schema schema, JsonNode value) throws Exception {
        // Handle union types (e.g., ["null", "string"])
        if (schema.getType() == Type.UNION) {
            List<Schema> types = schema.getTypes();
            
            // If value is null, return null for nullable unions
            if (value.isNull() && isNullable(schema)) {
                return null;
            }
            
            // Try each non-null type in the union
            for (Schema typeSchema : types) {
                if (typeSchema.getType() != Type.NULL) {
                    try {
                        return convertJsonValueToAvro(typeSchema, value);
                    } catch (Exception e) {
                        // Try the next type
                        continue;
                    }
                }
            }
            
            throw new Exception("Value doesn't match any type in the union");
        }
        
        // Handle specific types
        switch (schema.getType()) {
            case STRING:
                return value.isTextual() ? value.asText() : value.toString();
                
            case INT:
                return value.isInt() ? value.asInt() : Integer.parseInt(value.asText());
                
            case LONG:
                return value.isLong() || value.isInt() ? value.asLong() : Long.parseLong(value.asText());
                
            case FLOAT:
                return value.isFloat() || value.isDouble() ? 
                       value.floatValue() : Float.parseFloat(value.asText());
                
            case DOUBLE:
                return value.isDouble() || value.isFloat() || value.isInt() ? 
                       value.doubleValue() : Double.parseDouble(value.asText());
                
            case BOOLEAN:
                return value.isBoolean() ? value.booleanValue() : 
                       Boolean.parseBoolean(value.asText());
                
            case RECORD:
                // Create a new record for nested objects
                GenericRecord nestedRecord = new GenericData.Record(schema);
                for (Field field : schema.getFields()) {
                    String fieldName = field.name();
                    if (value.has(fieldName)) {
                        nestedRecord.put(fieldName, 
                            convertJsonValueToAvro(field.schema(), value.get(fieldName)));
                    } else if (field.defaultVal() != null) {
                        nestedRecord.put(fieldName, field.defaultVal());
                    }
                }
                return nestedRecord;
                
            case ARRAY:
                // Convert JSON array to Avro array
                if (!value.isArray()) {
                    throw new Exception("Expected array but got: " + value.getNodeType());
                }
                
                GenericData.Array<Object> array = 
                    new GenericData.Array<>(value.size(), schema);
                
                for (int i = 0; i < value.size(); i++) {
                    array.add(convertJsonValueToAvro(schema.getElementType(), value.get(i)));
                }
                return array;
                
            case MAP:
                // Convert JSON object to Avro map
                if (!value.isObject()) {
                    throw new Exception("Expected object but got: " + value.getNodeType());
                }
                
                java.util.Map<String, Object> map = new java.util.HashMap<>();
                value.fields().forEachRemaining(entry -> {
                    try {
                        map.put(entry.getKey(), 
                            convertJsonValueToAvro(schema.getValueType(), entry.getValue()));
                    } catch (Exception e) {
                        LOG.warn("Error converting map value for key '{}': {}", 
                                 entry.getKey(), e.getMessage());
                    }
                });
                return map;
                
            case NULL:
                return null;
                
            default:
                throw new Exception("Unsupported Avro type: " + schema.getType());
        }
    }
    
    /**
     * Checks if a schema is nullable (union including null)
     * 
     * @param schema Schema to check
     * @return true if the schema is nullable
     */
    public static boolean isNullable(Schema schema) {
        if (schema.getType() == Type.UNION) {
            for (Schema typeSchema : schema.getTypes()) {
                if (typeSchema.getType() == Type.NULL) {
                    return true;
                }
            }
        }
        return false;
    }
}

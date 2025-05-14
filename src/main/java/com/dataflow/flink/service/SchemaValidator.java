/**
 * Schema Validation Service Implementation
 * 
 * This file contains the SchemaValidator class which is responsible for validating
 * JSON messages against Apache Avro schemas.
 */
package com.dataflow.flink.service;

import com.dataflow.flink.model.ValidationResult;
import com.dataflow.flink.utils.AvroUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Service class for validating JSON data against Apache Avro schemas.
 * 
 * Apache Avro is a data serialization system that provides:
 * - Rich data structures
 * - A compact, fast, binary data format
 * - Schema-based validation
 * - Language-independent schema definition
 * 
 * This validator:
 * 1. Filters the input JSON to include only fields defined in the schema
 * 2. Converts the filtered JSON to an Avro GenericRecord
 * 3. Validates the GenericRecord against the schema
 * 4. Returns a ValidationResult with the outcome
 * 
 * Avro schemas define:
 * - Field names and their data types
 * - Whether fields are required or optional (nullable)
 * - Default values for fields
 * - Documentation for schemas and fields
 */
/**
 * Schema validator service that checks JSON messages against an Avro schema.
 */
public class SchemaValidator {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaValidator.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    /** The Avro schema used for validation */
    private final Schema schema;
    
    /**
     * Constructs a new SchemaValidator with the specified Avro schema.
     * 
     * The schema defines the expected structure of valid messages, including:
     * - Required and optional fields
     * - Data types for each field (string, int, array, etc.)
     * - Nested record structures
     * - Default values for missing fields
     *
     * @param schema The Apache Avro schema to validate against
     */
    public SchemaValidator(Schema schema) {
        this.schema = schema;
    }
    
    /**
     * Validates a JSON message against the Avro schema.
     * 
     * This method performs several steps to ensure valid validation:
     * 1. Filters the JSON to only include fields defined in the schema
     * 2. Converts the filtered JSON to an Avro GenericRecord
     * 3. Uses Avro's built-in validation to check the record against the schema
     * 4. Returns a ValidationResult with the outcome and relevant data
     * 
     * Validation logic handles:
     * - Type checking (is a string field actually a string?)
     * - Required fields (are all non-nullable fields present?)
     * - Enum values (are enum values in the allowed set?)
     * - Union types (for fields that can have multiple types)
     * 
     * @param jsonNode The JSON data to validate
     * @return ValidationResult containing validation outcome, processed data, and error message if invalid
     */
    public ValidationResult validate(JsonNode jsonNode) {
        try {
            // Filter out fields not in the schema
            // This prevents unknown fields from causing validation failures
            ObjectNode filteredJson = filterFields(jsonNode);
            
            // Convert filtered JSON to Avro GenericRecord
            // GenericRecord is Avro's internal representation of data
            GenericRecord record = jsonToAvro(filteredJson);
            
            // Validate the record against the schema using Avro's validator
            // This checks for type compatibility, required fields, and other constraints
            boolean isValid = GenericData.get().validate(schema, record);
            
            if (isValid) {
                // If valid, return the filtered JSON (fields matching the schema)
                return new ValidationResult(true, filteredJson, null);
            } else {
                // If invalid, log and return with an error message
                String errorMessage = "Record failed Avro schema validation";
                LOG.warn("Validation failed: {}", errorMessage);
                return new ValidationResult(false, jsonNode, errorMessage);
            }
        } catch (Exception e) {
            // Handle any exceptions during validation (type conversion errors, etc.)
            LOG.warn("Validation error: {}", e.getMessage());
            return new ValidationResult(false, jsonNode, "Validation error: " + e.getMessage());
        }
    }
    
    /**
     * Filters JSON fields to only include those defined in the schema.
     * 
     * This method:
     * 1. Creates an empty JSON object to hold filtered fields
     * 2. Builds a map of field names from the schema for quick lookup
     * 3. Iterates through all fields in the input JSON
     * 4. Copies only fields that exist in the schema to the filtered JSON
     * 
     * Filtering is important because:
     * - It prevents unknown fields from causing validation failures
     * - It focuses validation on the fields we care about
     * - It prepares the data for clean conversion to Avro format
     *
     * @param jsonNode Original JSON data with potentially extra fields
     * @return ObjectNode with only the fields defined in the schema
     */
    private ObjectNode filterFields(JsonNode jsonNode) {
        ObjectNode filteredJson = objectMapper.createObjectNode();
        Map<String, Field> schemaFields = new HashMap<>();
        
        // Build map of schema field names for quick lookup
        // This improves performance for schemas with many fields
        for (Field field : schema.getFields()) {
            schemaFields.put(field.name(), field);
        }
        
        // Copy only fields that exist in the schema
        // This removes any fields not defined in the schema
        Iterator<Map.Entry<String, JsonNode>> fields = jsonNode.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String fieldName = field.getKey();
            
            if (schemaFields.containsKey(fieldName)) {
                filteredJson.set(fieldName, field.getValue());
            }
        }
        
        return filteredJson;
    }
    
    /**
     * Converts JSON to Avro GenericRecord for validation.
     * 
     * Avro GenericRecord is the internal representation that Avro uses for:
     * - Validation against schema
     * - Serialization to binary format
     * - Type checking and conversion
     * 
     * This method delegates to the AvroUtil utility class which handles:
     * - Type conversions (JSON to Avro types)
     * - Nested structure handling
     * - Array and map conversions
     * - Null value handling
     *
     * @param jsonNode JSON data to convert
     * @return GenericRecord representation of the JSON data
     * @throws Exception if conversion fails due to type incompatibility
     */
    private GenericRecord jsonToAvro(JsonNode jsonNode) throws Exception {
        return AvroUtil.convertJsonToAvro(schema, jsonNode);
    }
    
    /**
     * Gets the schema used by this validator.
     * 
     * The schema is the contract that defines what makes a message valid.
     * It specifies field names, types, and constraints.
     *
     * @return Avro schema used for validation
     */
    public Schema getSchema() {
        return schema;
    }
}

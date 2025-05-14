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
 * Service to validate JSON data against Avro schema
 */
public class SchemaValidator {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaValidator.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    private final Schema schema;
    
    public SchemaValidator(Schema schema) {
        this.schema = schema;
    }
    
    /**
     * Validates JSON data against Avro schema
     * 
     * @param jsonNode JSON data to validate
     * @return ValidationResult containing validation result and processed data
     */
    public ValidationResult validate(JsonNode jsonNode) {
        try {
            // Filter out fields not in the schema
            ObjectNode filteredJson = filterFields(jsonNode);
            
            // Convert filtered JSON to Avro GenericRecord
            GenericRecord record = jsonToAvro(filteredJson);
            
            // Validate the record against the schema
            boolean isValid = GenericData.get().validate(schema, record);
            
            if (isValid) {
                return new ValidationResult(true, filteredJson, null);
            } else {
                String errorMessage = "Record failed Avro schema validation";
                LOG.warn("Validation failed: {}", errorMessage);
                return new ValidationResult(false, jsonNode, errorMessage);
            }
        } catch (Exception e) {
            LOG.warn("Validation error: {}", e.getMessage());
            return new ValidationResult(false, jsonNode, "Validation error: " + e.getMessage());
        }
    }
    
    /**
     * Filters JSON fields to only include those defined in the schema
     * 
     * @param jsonNode Original JSON data
     * @return ObjectNode with only the fields defined in the schema
     */
    private ObjectNode filterFields(JsonNode jsonNode) {
        ObjectNode filteredJson = objectMapper.createObjectNode();
        Map<String, Field> schemaFields = new HashMap<>();
        
        // Build map of schema field names for quick lookup
        for (Field field : schema.getFields()) {
            schemaFields.put(field.name(), field);
        }
        
        // Copy only fields that exist in the schema
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
     * Converts JSON to Avro GenericRecord
     * 
     * @param jsonNode JSON data to convert
     * @return GenericRecord representation of the JSON data
     * @throws Exception if conversion fails
     */
    private GenericRecord jsonToAvro(JsonNode jsonNode) throws Exception {
        return AvroUtil.convertJsonToAvro(schema, jsonNode);
    }
    
    /**
     * Gets the schema used by this validator
     * 
     * @return Avro schema
     */
    public Schema getSchema() {
        return schema;
    }
}

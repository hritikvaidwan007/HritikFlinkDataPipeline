package com.dataflow.flink.service;

import com.dataflow.flink.model.ValidationResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.Schema;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class SchemaValidatorTest {
    
    private SchemaValidator validator;
    private ObjectMapper objectMapper;
    private Schema schema;
    
    @Before
    public void setUp() throws Exception {
        // Define a simple schema for testing
        String schemaJson = "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"TestRecord\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"id\", \"type\": [\"null\", \"int\"], \"default\": null},\n" +
                "    {\"name\": \"name\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"active\", \"type\": [\"null\", \"boolean\"], \"default\": null}\n" +
                "  ]\n" +
                "}";
        
        schema = new Schema.Parser().parse(schemaJson);
        validator = new SchemaValidator(schema);
        objectMapper = new ObjectMapper();
    }
    
    @Test
    public void testValidRecord() throws Exception {
        // Create a valid JSON node
        ObjectNode node = objectMapper.createObjectNode();
        node.put("id", 123);
        node.put("name", "Test User");
        node.put("active", true);
        
        ValidationResult result = validator.validate(node);
        
        assertTrue("Valid record should pass validation", result.isValid());
        assertNull("Error message should be null for valid record", result.getErrorMessage());
        assertNotNull("Data should not be null", result.getData());
    }
    
    @Test
    public void testInvalidRecord() throws Exception {
        // Create an invalid JSON node (wrong type for 'id')
        ObjectNode node = objectMapper.createObjectNode();
        node.put("id", "not-an-integer");
        node.put("name", "Test User");
        node.put("active", true);
        
        ValidationResult result = validator.validate(node);
        
        assertFalse("Invalid record should fail validation", result.isValid());
        assertNotNull("Error message should not be null for invalid record", result.getErrorMessage());
        assertNotNull("Data should not be null", result.getData());
    }
    
    @Test
    public void testExtraFields() throws Exception {
        // Create a JSON node with extra fields
        ObjectNode node = objectMapper.createObjectNode();
        node.put("id", 123);
        node.put("name", "Test User");
        node.put("active", true);
        node.put("extraField", "should be ignored");
        
        ValidationResult result = validator.validate(node);
        
        assertTrue("Record with extra fields should pass validation", result.isValid());
        assertNull("Error message should be null", result.getErrorMessage());
        
        // Verify extra field was filtered out
        JsonNode data = result.getData();
        assertFalse("Extra field should be removed", data.has("extraField"));
        assertEquals("Original fields should be preserved", 123, data.get("id").asInt());
    }
    
    @Test
    public void testMissingFields() throws Exception {
        // Create a JSON node with missing fields
        ObjectNode node = objectMapper.createObjectNode();
        node.put("id", 123);
        // Missing 'name' and 'active' fields
        
        ValidationResult result = validator.validate(node);
        
        assertTrue("Record with missing fields should pass validation (defaults applied)", result.isValid());
        assertNull("Error message should be null", result.getErrorMessage());
    }
}

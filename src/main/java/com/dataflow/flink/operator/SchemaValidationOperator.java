package com.dataflow.flink.operator;

import com.dataflow.flink.model.ValidationResult;
import com.dataflow.flink.service.SchemaValidator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flink ProcessFunction that validates JSON messages against Avro schema
 */
public class SchemaValidationOperator extends ProcessFunction<JsonNode, ValidationResult> {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaValidationOperator.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    private final SchemaValidator validator;
    private final OutputTag<String> invalidTag;
    
    private long validCount = 0;
    private long invalidCount = 0;
    private long totalCount = 0;
    private long lastLogTime = 0;
    private static final long LOG_INTERVAL_MS = 60000; // Log statistics every minute
    
    public SchemaValidationOperator(SchemaValidator validator, OutputTag<String> invalidTag) {
        this.validator = validator;
        this.invalidTag = invalidTag;
    }
    
    @Override
    public void processElement(JsonNode value, Context ctx, Collector<ValidationResult> out) throws Exception {
        totalCount++;
        
        try {
            // Validate the message against the schema
            ValidationResult result = validator.validate(value);
            
            if (result.isValid()) {
                // Send valid messages to the main output
                validCount++;
                out.collect(result);
            } else {
                // Send invalid messages to the side output
                invalidCount++;
                
                // Create error message with the original data
                JsonNode errorData = value.deepCopy();
                if (errorData.isObject()) {
                    ((com.fasterxml.jackson.databind.node.ObjectNode) errorData)
                        .put("_error", result.getErrorMessage())
                        .put("_timestamp", System.currentTimeMillis());
                }
                
                ctx.output(invalidTag, objectMapper.writeValueAsString(errorData));
            }
            
            // Log statistics periodically
            long currentTime = System.currentTimeMillis();
            if (currentTime - lastLogTime > LOG_INTERVAL_MS) {
                LOG.info("Processing statistics - Total: {}, Valid: {}, Invalid: {}, Validation Rate: {}%", 
                        totalCount, validCount, invalidCount, 
                        totalCount > 0 ? (validCount * 100.0 / totalCount) : 0);
                lastLogTime = currentTime;
            }
        } catch (Exception e) {
            // Handle any unexpected exceptions
            LOG.error("Error processing message: {}", e.getMessage(), e);
            invalidCount++;
            
            // Create error message for the exception
            com.fasterxml.jackson.databind.node.ObjectNode errorNode = objectMapper.createObjectNode();
            errorNode.set("original_message", value);
            errorNode.put("_error", "Processing error: " + e.getMessage());
            errorNode.put("_timestamp", System.currentTimeMillis());
            
            ctx.output(invalidTag, objectMapper.writeValueAsString(errorNode));
        }
    }
}

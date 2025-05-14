/**
 * Apache Flink Schema Validation Operator Implementation
 * 
 * This file contains the SchemaValidationOperator class, which is a custom Flink operator
 * for validating incoming JSON messages against an Avro schema.
 */
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
 * A custom Apache Flink ProcessFunction that validates JSON messages against an Avro schema.
 * 
 * In Apache Flink, a ProcessFunction is a low-level stream processing operation that gives
 * access to:
 * - Process individual events (with full type information)
 * - State (fault-tolerant storage)
 * - Side outputs (multiple output streams)
 * - Event time and processing time
 * 
 * This operator:
 * 1. Takes JSON messages as input
 * 2. Validates them against the provided Avro schema
 * 3. Routes valid messages to the main output
 * 4. Routes invalid messages to a side output for error handling
 * 5. Maintains and logs statistics about validation success/failure rates
 */
/**
 * Custom ProcessFunction that validates JSON messages against an Avro schema.
 * 
 * The class extends Flink's ProcessFunction, which provides access to:
 * - The current element being processed (JsonNode)
 * - The context of the processing (for side outputs and other metadata)
 * - A collector for emitting results (ValidationResult)
 */
public class SchemaValidationOperator extends ProcessFunction<JsonNode, ValidationResult> {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaValidationOperator.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    /** Schema validator service that performs the actual validation logic */
    private final transient SchemaValidator validator;
    
    /** OutputTag for routing invalid messages to a side output stream */
    private final OutputTag<String> invalidTag;
    
    /** Counters for monitoring and reporting validation statistics */
    private long validCount = 0;
    private long invalidCount = 0;
    private long totalCount = 0;
    private long lastLogTime = 0;
    
    /** Time interval (in ms) for logging processing statistics */
    private static final long LOG_INTERVAL_MS = 60000; // Log statistics every minute
    
    /**
     * Constructor for the SchemaValidationOperator.
     *
     * @param validator The schema validator service that will perform validation
     * @param invalidTag The output tag to use for invalid messages (side output)
     */
    public SchemaValidationOperator(SchemaValidator validator, OutputTag<String> invalidTag) {
        this.validator = validator;
        this.invalidTag = invalidTag;
    }
    
    /**
     * Core processing method that validates each message against the schema.
     *
     * This method is called for each element in the input stream. It:
     * 1. Validates the input JSON against the schema
     * 2. Routes valid messages to the main output stream
     * 3. Routes invalid messages to the side output stream
     * 4. Maintains validation statistics
     * 5. Logs processing statistics periodically
     *
     * In Flink, the processElement method is the heart of stream processing,
     * allowing fine-grained control over how each element is handled.
     *
     * @param value The JSON message to validate
     * @param ctx Processing context (provides access to side outputs, timestamps, etc.)
     * @param out Collector for emitting valid results to the main output
     * @throws Exception If an error occurs during processing
     */
    @Override
    public void processElement(JsonNode value, Context ctx, Collector<ValidationResult> out) throws Exception {
        totalCount++;
        
        try {
            // Validate the message against the schema
            // This delegates to the SchemaValidator service
            ValidationResult result = validator.validate(value);
            
            if (result.isValid()) {
                // For valid messages:
                // 1. Increment valid counter
                // 2. Emit to main output using the collector
                validCount++;
                out.collect(result);
            } else {
                // For invalid messages:
                // 1. Increment invalid counter
                // 2. Enrich error data with validation details
                // 3. Emit to side output using the context
                invalidCount++;
                
                // Create error message with the original data and error details
                // This preserves the original message while adding error metadata
                JsonNode errorData = value.deepCopy();
                if (errorData.isObject()) {
                    ((com.fasterxml.jackson.databind.node.ObjectNode) errorData)
                        .put("_error", result.getErrorMessage())
                        .put("_timestamp", System.currentTimeMillis());
                }
                
                // Side outputs in Flink allow a single operator to produce multiple output streams
                // Here we use it to separate valid and invalid messages
                ctx.output(invalidTag, objectMapper.writeValueAsString(errorData));
            }
            
            // Log statistics periodically for monitoring
            // This provides visibility into the validation process without excessive logging
            long currentTime = System.currentTimeMillis();
            if (currentTime - lastLogTime > LOG_INTERVAL_MS) {
                LOG.info("Processing statistics - Total: {}, Valid: {}, Invalid: {}, Validation Rate: {}%", 
                        totalCount, validCount, invalidCount, 
                        totalCount > 0 ? (validCount * 100.0 / totalCount) : 0);
                lastLogTime = currentTime;
            }
        } catch (Exception e) {
            // Handle any unexpected exceptions during processing
            // This ensures the Flink job doesn't fail if a single message causes problems
            LOG.error("Error processing message: {}", e.getMessage(), e);
            invalidCount++;
            
            // Create an error message that includes both the original data and exception details
            // This helps with debugging and troubleshooting validation issues
            com.fasterxml.jackson.databind.node.ObjectNode errorNode = objectMapper.createObjectNode();
            errorNode.set("original_message", value);
            errorNode.put("_error", "Processing error: " + e.getMessage());
            errorNode.put("_timestamp", System.currentTimeMillis());
            
            // Route the error to the side output
            ctx.output(invalidTag, objectMapper.writeValueAsString(errorNode));
        }
    }
}

/**
 * Simplified local execution mode to validate schemas without using Flink runtime
 * when running in environments with Java module restrictions. 
 * 
 * This class bypasses the Flink streaming API for local testing, providing 
 * a direct implementation to process files using standard Java I/O.
 */
package com.dataflow.flink;

import com.dataflow.flink.config.AppConfig;
import com.dataflow.flink.config.SchemaConfig;
import com.dataflow.flink.model.ValidationResult;
import com.dataflow.flink.service.SchemaFetchService;
import com.dataflow.flink.service.SchemaValidator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Local execution class for testing schema validation without using Flink runtime.
 * This provides a simple implementation for direct file processing that avoids
 * Java module access issues in environments like Replit.
 */
public class LocalRunner {
    private static final Logger LOG = LoggerFactory.getLogger(LocalRunner.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    /**
     * Execute schema validation on local files without using Flink
     * 
     * @param appConfig Application configuration
     * @return True if execution was successful, false otherwise
     */
    public static boolean execute(AppConfig appConfig, SchemaConfig schemaConfig) {
        try {
            LOG.info("Running in local execution mode (bypassing Flink runtime)");
            
            // Validate inputs
            if (!Files.exists(Paths.get(appConfig.getInputFilePath()))) {
                LOG.error("Input file does not exist: {}", appConfig.getInputFilePath());
                return false;
            }
            
            // Fetch schema
            Schema avroSchema;
            SchemaFetchService schemaService = new SchemaFetchService(schemaConfig, appConfig.getEnvironment());
            avroSchema = schemaService.fetchSchema();
            
            // Create schema validator
            SchemaValidator validator = new SchemaValidator(avroSchema);
            
            // Prepare output files
            Path validOutputPath = Paths.get(appConfig.getOutputFilePath());
            Path invalidOutputPath = Paths.get(appConfig.getOutputFilePath() + ".invalid");
            
            // Ensure output directory exists
            Files.createDirectories(validOutputPath.getParent());
            
            // Delete existing output files if they exist
            Files.deleteIfExists(validOutputPath);
            Files.deleteIfExists(invalidOutputPath);
            
            // Process input file line by line
            List<String> lines = Files.readAllLines(Paths.get(appConfig.getInputFilePath()));
            LOG.info("Processing {} lines from {}", lines.size(), appConfig.getInputFilePath());
            
            int validCount = 0;
            int invalidCount = 0;
            
            List<String> validResults = new ArrayList<>();
            List<String> invalidResults = new ArrayList<>();
            
            for (String line : lines) {
                try {
                    // Parse JSON
                    JsonNode jsonNode = objectMapper.readTree(line);
                    
                    // Validate against schema
                    ValidationResult result = validator.validate(jsonNode);
                    
                    if (result.isValid()) {
                        // Handle valid message
                        validCount++;
                        validResults.add(objectMapper.writeValueAsString(result.getData()));
                        LOG.debug("Valid message: {}", objectMapper.writeValueAsString(result.getData()));
                    } else {
                        // Handle invalid message
                        invalidCount++;
                        String errorMessage = result.getErrorMessage();
                        invalidResults.add(errorMessage);
                        LOG.debug("Invalid message: {}", errorMessage);
                    }
                } catch (Exception e) {
                    // Handle parsing errors
                    invalidCount++;
                    String errorMessage = String.format("Error processing message: %s", e.getMessage());
                    invalidResults.add(errorMessage);
                    LOG.error(errorMessage, e);
                }
            }
            
            // Write results to files
            if (!validResults.isEmpty()) {
                Files.write(validOutputPath, validResults);
                LOG.info("Wrote {} valid messages to {}", validCount, validOutputPath);
            }
            
            if (!invalidResults.isEmpty()) {
                Files.write(invalidOutputPath, invalidResults);
                LOG.info("Wrote {} invalid messages to {}", invalidCount, invalidOutputPath);
            }
            
            LOG.info("Local execution completed successfully: {} valid, {} invalid", validCount, invalidCount);
            return true;
            
        } catch (Exception e) {
            LOG.error("Error in local execution: {}", e.getMessage(), e);
            return false;
        }
    }
}
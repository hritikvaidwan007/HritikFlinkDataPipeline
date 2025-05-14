package com.dataflow.flink;

import com.dataflow.flink.config.AppConfig;
import com.dataflow.flink.config.ConfigLoader;
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
 * Example class demonstrating the local file sink functionality
 * integrated with the actual schema validation logic.
 * 
 * This class:
 * 1. Loads configuration from local.properties
 * 2. Processes sample data from a file source
 * 3. Validates each record against a schema
 * 4. Writes valid records to the output file
 * 5. Writes invalid records to the .invalid file
 * 
 * Unlike SimpleDemoWriter, this class actually validates
 * the data against the schema.
 */
public class LocalFileSinkExample {
    private static final Logger LOG = LoggerFactory.getLogger(LocalFileSinkExample.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    public static void main(String[] args) {
        try {
            LOG.info("Starting Local File Sink Example...");
            
            // Load configuration from local.properties
            String configPath = "config/local.properties";
            LOG.info("Loading configuration from: {}", configPath);
            AppConfig appConfig = ConfigLoader.loadConfig(configPath);
            SchemaConfig schemaConfig = appConfig.getSchemaConfig();
            
            // Set file paths based on config
            String inputPath = appConfig.getInputFilePath();
            String outputPath = appConfig.getOutputFilePath();
            String invalidPath = outputPath + ".invalid";
            
            LOG.info("Using file source: {}", inputPath);
            LOG.info("Using file sink for valid records: {}", outputPath);
            LOG.info("Using file sink for invalid records: {}", invalidPath);
            
            // Fetch schema
            LOG.info("Loading schema...");
            Schema avroSchema;
            try {
                SchemaFetchService schemaService = new SchemaFetchService(schemaConfig, appConfig.getEnvironment());
                avroSchema = schemaService.fetchSchema();
                LOG.info("Schema loaded successfully: {}", avroSchema.getName());
            } catch (Exception e) {
                LOG.warn("Failed to load schema from API, forcing local schema: {}", e.getMessage());
                
                // Override to use local schema if API fails
                schemaConfig.setUseLocalSchema(true);
                SchemaFetchService localSchemaService = new SchemaFetchService(schemaConfig, "local");
                avroSchema = localSchemaService.fetchSchema();
                LOG.info("Local schema loaded as fallback: {}", avroSchema.getName());
            }
            
            // Create schema validator
            SchemaValidator validator = new SchemaValidator(avroSchema);
            
            // Ensure output directory exists
            Files.createDirectories(Paths.get(outputPath).getParent());
            
            // Delete existing output files
            Files.deleteIfExists(Paths.get(outputPath));
            Files.deleteIfExists(Paths.get(invalidPath));
            
            // Process input file
            LOG.info("Reading input data from: {}", inputPath);
            List<String> lines = Files.readAllLines(Paths.get(inputPath));
            LOG.info("Processing {} records...", lines.size());
            
            List<String> validResults = new ArrayList<>();
            List<String> invalidResults = new ArrayList<>();
            
            int validCount = 0;
            int invalidCount = 0;
            
            for (String line : lines) {
                try {
                    // Parse JSON
                    JsonNode jsonNode = objectMapper.readTree(line);
                    
                    // Validate against schema
                    ValidationResult result = validator.validate(jsonNode);
                    
                    if (result.isValid()) {
                        // Handle valid message
                        String validJson = objectMapper.writeValueAsString(result.getData());
                        validResults.add(validJson);
                        LOG.info("Valid record: {}", validJson);
                        validCount++;
                    } else {
                        // Handle invalid message
                        String errorMessage = result.getErrorMessage();
                        invalidResults.add(errorMessage);
                        LOG.info("Invalid record: {}", errorMessage);
                        invalidCount++;
                    }
                } catch (Exception e) {
                    // Handle parsing errors
                    String errorMessage = String.format("Error processing record: %s", e.getMessage());
                    invalidResults.add(errorMessage);
                    LOG.error(errorMessage, e);
                    invalidCount++;
                }
            }
            
            // Write results to files
            if (!validResults.isEmpty()) {
                Files.write(Paths.get(outputPath), validResults);
                LOG.info("Wrote {} valid records to {}", validCount, outputPath);
            }
            
            if (!invalidResults.isEmpty()) {
                Files.write(Paths.get(invalidPath), invalidResults);
                LOG.info("Wrote {} invalid records to {}", invalidCount, invalidPath);
            }
            
            LOG.info("Local File Sink Example completed successfully");
            LOG.info("Summary: {} valid records, {} invalid records", validCount, invalidCount);
            
        } catch (Exception e) {
            LOG.error("Error in Local File Sink Example: {}", e.getMessage(), e);
        }
    }
}
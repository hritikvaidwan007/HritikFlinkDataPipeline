#!/bin/bash

# Simple script to run the application in local mode with simplified processing
# This bypasses Flink runtime to test validation functionality

echo "Building the application..."
mvn clean package -DskipTests

echo "Running schema validation in local mode..."
CLASSPATH="target/flink-schema-validator-1.0.0-shaded.jar"

# Create simple test runner
cat > src/main/java/com/dataflow/flink/SimpleLocalRunner.java << EOF
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

import java.io.*;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

public class SimpleLocalRunner {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    public static void main(String[] args) {
        try {
            System.out.println("Starting simple local schema validation...");
            String configPath = "config/local.properties";
            if (args.length > 0) {
                configPath = args[0];
            }
            
            // Load configuration
            AppConfig appConfig = ConfigLoader.loadConfig(configPath);
            SchemaConfig schemaConfig = appConfig.getSchemaConfig();
            
            System.out.println("Loading schema from: " + 
                (schemaConfig.isUseLocal() ? schemaConfig.getLocalPath() : schemaConfig.getApiUrl()));
            
            // Fetch schema
            SchemaFetchService schemaService = new SchemaFetchService(schemaConfig);
            Schema avroSchema = schemaService.fetchSchema();
            System.out.println("Schema loaded successfully: " + avroSchema.getName());
            
            // Create schema validator
            SchemaValidator validator = new SchemaValidator(avroSchema);
            
            // Prepare paths
            String inputPath = appConfig.getInputFilePath();
            String outputPath = appConfig.getOutputFilePath();
            String invalidPath = outputPath + ".invalid";
            
            System.out.println("Reading data from: " + inputPath);
            System.out.println("Writing valid results to: " + outputPath);
            System.out.println("Writing invalid results to: " + invalidPath);
            
            // Ensure output directory exists
            Files.createDirectories(Paths.get(outputPath).getParent());
            
            // Delete existing output files
            Files.deleteIfExists(Paths.get(outputPath));
            Files.deleteIfExists(Paths.get(invalidPath));
            
            // Process input file
            List<String> lines = Files.readAllLines(Paths.get(inputPath));
            System.out.println("Processing " + lines.size() + " messages...");
            
            List<String> validResults = new ArrayList<>();
            List<String> invalidResults = new ArrayList<>();
            
            for (String line : lines) {
                JsonNode jsonNode = objectMapper.readTree(line);
                ValidationResult result = validator.validate(jsonNode);
                
                if (result.isValid()) {
                    validResults.add(objectMapper.writeValueAsString(result.getData()));
                } else {
                    invalidResults.add(result.getErrorMessage());
                }
            }
            
            // Write results
            if (!validResults.isEmpty()) {
                Files.write(Paths.get(outputPath), validResults);
                System.out.println("Wrote " + validResults.size() + " valid messages");
            }
            
            if (!invalidResults.isEmpty()) {
                Files.write(Paths.get(invalidPath), invalidResults);
                System.out.println("Wrote " + invalidResults.size() + " invalid messages");
            }
            
            System.out.println("Schema validation completed successfully!");
            
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
EOF

# Compile and run
mvn compile
java -cp $CLASSPATH com.dataflow.flink.SimpleLocalRunner config/local.properties

echo "Done!"
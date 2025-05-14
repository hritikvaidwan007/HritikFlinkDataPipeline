/**
 * Schema Registry Client Implementation
 * 
 * This file contains the SchemaFetchService, which is responsible for retrieving
 * Avro schemas either from a remote Schema Registry API or from a local file.
 */
package com.dataflow.flink.service;

import com.dataflow.flink.config.SchemaConfig;
import com.dataflow.flink.model.SchemaResponse;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Service for fetching Apache Avro schemas from either a remote API (Schema Registry) or a local file.
 * 
 * In a production streaming data pipeline, schemas are typically stored in a Schema Registry,
 * which is a central repository for schemas that provides:
 * 
 * - Schema versioning: Tracking changes to schemas over time
 * - Schema evolution: Rules for handling schema changes
 * - Schema compatibility: Ensuring readers and writers can communicate
 * - Schema discovery: Finding schemas for specific data types
 * 
 * Popular Schema Registry implementations include:
 * - Confluent Schema Registry
 * - AWS Glue Schema Registry
 * - Google Cloud Schema Registry
 * - IBM Event Streams Schema Registry
 * 
 * This service provides two ways to obtain a schema:
 * 1. From a Schema Registry API (for production)
 * 2. From a local file (for development/testing)
 */
/**
 * Service for fetching and parsing Avro schemas from either a Schema Registry API or local files.
 */
public class SchemaFetchService {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaFetchService.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    /** Configuration for schema fetching behavior and locations */
    private final SchemaConfig schemaConfig;
    
    /** HTTP client for API requests to the Schema Registry */
    private final OkHttpClient httpClient;
    
    /**
     * Constructs a new SchemaFetchService with the specified configuration.
     * 
     * This initializes an HTTP client with appropriate timeouts for API calls.
     * In a production environment, schema registry calls should have reasonable
     * timeouts to prevent application startup delays or pipeline stalls.
     *
     * @param schemaConfig Configuration containing schema fetch parameters
     */
    public SchemaFetchService(SchemaConfig schemaConfig) {
        this.schemaConfig = schemaConfig;
        
        // Configure HTTP client with reasonable timeouts
        // In production environments, these values may need adjustment based on network conditions
        // and Schema Registry performance characteristics
        this.httpClient = new OkHttpClient.Builder()
                .connectTimeout(30, TimeUnit.SECONDS) // Time to establish connection
                .readTimeout(30, TimeUnit.SECONDS)    // Time to receive data
                .writeTimeout(30, TimeUnit.SECONDS)   // Time to send data
                .build();
    }
    
    /**
     * Main method that fetches the Avro schema from either an API or local file.
     * 
     * This method serves as the primary entry point for obtaining a schema. It:
     * 1. Determines the schema source (API or file) based on configuration
     * 2. Delegates to the appropriate method to fetch the schema text
     * 3. Parses the schema text into an Avro Schema object
     * 4. Logs the results for monitoring and troubleshooting
     * 
     * In Avro, a Schema object represents the structure of data and is used for:
     * - Validation of data against the schema
     * - Serialization and deserialization of data
     * - Schema evolution and compatibility checks
     *
     * @return Avro Schema object ready for validation use
     * @throws Exception if schema cannot be fetched or parsed
     */
    public Schema fetchSchema() throws Exception {
        String schemaText;
        
        // Choose the source based on configuration
        // Local files are typically used for development/testing
        // Remote APIs are used for production deployments
        if (schemaConfig.isUseLocalSchema()) {
            LOG.info("Using local schema file: {}", schemaConfig.getLocalSchemaPath());
            schemaText = loadSchemaFromFile();
        } else {
            LOG.info("Fetching schema from API: {}", schemaConfig.getSchemaApiUrl());
            schemaText = fetchSchemaFromApi();
        }
        
        // Parse schema text into Avro Schema object
        // The Schema.Parser handles the conversion from JSON schema definition
        // to a compiled Schema object that can be used for validation
        Schema avroSchema = new Schema.Parser().parse(schemaText);
        LOG.info("Schema loaded successfully with name: {}", avroSchema.getName());
        return avroSchema;
    }
    
    /**
     * Loads an Avro schema from a local file.
     * 
     * This method is primarily used for development and testing scenarios where:
     * - The schema is being actively developed
     * - There is no Schema Registry available
     * - Offline testing is required
     * 
     * The file should contain a valid Avro schema in JSON format.
     * Example schema file content:
     * {
     *   "type": "record",
     *   "name": "User",
     *   "fields": [
     *     {"name": "id", "type": "string"},
     *     {"name": "name", "type": "string"},
     *     {"name": "email", "type": ["null", "string"]}
     *   ]
     * }
     *
     * @return String containing the Avro schema definition in JSON format
     * @throws IOException if the file cannot be read or doesn't exist
     */
    private String loadSchemaFromFile() throws IOException {
        String path = schemaConfig.getLocalSchemaPath();
        try {
            // Read the entire file into a string
            // This is appropriate for schema files which are typically small
            return new String(Files.readAllBytes(Paths.get(path)));
        } catch (IOException e) {
            LOG.error("Failed to read schema from file: {}", path, e);
            throw e;
        }
    }
    
    /**
     * Fetches an Avro schema from a remote Schema Registry API.
     * 
     * This method:
     * 1. Makes an HTTP request to the configured Schema Registry URL
     * 2. Parses the response into a SchemaResponse object
     * 3. Finds the requested schema version or falls back to the latest
     * 4. Extracts and returns the schema text
     * 
     * Schema versioning is a critical concept in data streaming:
     * - It allows schemas to evolve over time
     * - It supports backward and forward compatibility
     * - It enables tracking of schema changes
     * 
     * This implementation supports:
     * - Fetching a specific version by number
     * - Falling back to the latest version if specified version is not found
     * - Proper error handling for API failures
     *
     * @return String containing the Avro schema definition in JSON format
     * @throws Exception if the API call fails or the response cannot be parsed
     */
    private String fetchSchemaFromApi() throws Exception {
        // Build the HTTP request to the Schema Registry API
        Request request = new Request.Builder()
                .url(schemaConfig.getSchemaApiUrl())
                .get()
                .build();
        
        try (Response response = httpClient.newCall(request).execute()) {
            // Check for HTTP error responses
            if (!response.isSuccessful()) {
                String errorMsg = "Failed to fetch schema: " + response.code() + " - " + response.message();
                LOG.error(errorMsg);
                throw new IOException(errorMsg);
            }
            
            // Parse the response body into our schema response model
            String responseBody = response.body().string();
            SchemaResponse schemaResponse = objectMapper.readValue(responseBody, SchemaResponse.class);
            
            // Find the requested schema version or use the latest one
            // This supports schema evolution by allowing specific versions to be requested
            JsonNode schemaJson = null;
            Integer requestedVersion = schemaConfig.getSchemaVersion();
            
            if (requestedVersion != null) {
                LOG.info("Looking for specific schema version: {}", requestedVersion);
                
                // Search for the requested version in the available versions
                for (JsonNode versionNode : schemaResponse.getSchemas().get("versions")) {
                    int version = versionNode.get("version").asInt();
                    if (version == requestedVersion) {
                        String schemaText = versionNode.get("schemaText").asText();
                        LOG.info("Found requested schema version: {}", requestedVersion);
                        return schemaText;
                    }
                }
                
                // If requested version is not found, log a warning and fall back
                LOG.warn("Requested schema version {} not found, falling back to latest version", requestedVersion);
            }
            
            // Use the latest version (first in the array)
            // In many Schema Registry implementations, versions are ordered by recency
            if (schemaResponse.getSchemas().get("versions").size() > 0) {
                String schemaText = schemaResponse.getSchemas().get("versions").get(0).get("schemaText").asText();
                int version = schemaResponse.getSchemas().get("versions").get(0).get("version").asInt();
                LOG.info("Using schema version: {}", version);
                return schemaText;
            } else {
                throw new Exception("No schema versions found in the response");
            }
        } catch (Exception e) {
            LOG.error("Failed to fetch schema from API", e);
            throw e;
        }
    }
}

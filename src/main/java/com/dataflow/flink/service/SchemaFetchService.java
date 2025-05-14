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
 * Service to fetch Avro schema from API or local file
 */
public class SchemaFetchService {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaFetchService.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    private final SchemaConfig schemaConfig;
    private final OkHttpClient httpClient;
    
    public SchemaFetchService(SchemaConfig schemaConfig) {
        this.schemaConfig = schemaConfig;
        
        // Configure HTTP client with reasonable timeouts
        this.httpClient = new OkHttpClient.Builder()
                .connectTimeout(30, TimeUnit.SECONDS)
                .readTimeout(30, TimeUnit.SECONDS)
                .writeTimeout(30, TimeUnit.SECONDS)
                .build();
    }
    
    /**
     * Fetches Avro schema from API or local file
     * 
     * @return Avro Schema object
     * @throws Exception if schema cannot be fetched or parsed
     */
    public Schema fetchSchema() throws Exception {
        String schemaText;
        
        if (schemaConfig.isUseLocalSchema()) {
            LOG.info("Using local schema file: {}", schemaConfig.getLocalSchemaPath());
            schemaText = loadSchemaFromFile();
        } else {
            LOG.info("Fetching schema from API: {}", schemaConfig.getSchemaApiUrl());
            schemaText = fetchSchemaFromApi();
        }
        
        // Parse schema text into Avro Schema object
        Schema avroSchema = new Schema.Parser().parse(schemaText);
        LOG.info("Schema loaded successfully with name: {}", avroSchema.getName());
        return avroSchema;
    }
    
    /**
     * Loads Avro schema from local file
     * 
     * @return String containing Avro schema definition
     * @throws IOException if file cannot be read
     */
    private String loadSchemaFromFile() throws IOException {
        String path = schemaConfig.getLocalSchemaPath();
        try {
            return new String(Files.readAllBytes(Paths.get(path)));
        } catch (IOException e) {
            LOG.error("Failed to read schema from file: {}", path, e);
            throw e;
        }
    }
    
    /**
     * Fetches Avro schema from remote API
     * 
     * @return String containing Avro schema definition
     * @throws Exception if API call fails or response cannot be parsed
     */
    private String fetchSchemaFromApi() throws Exception {
        Request request = new Request.Builder()
                .url(schemaConfig.getSchemaApiUrl())
                .get()
                .build();
        
        try (Response response = httpClient.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                String errorMsg = "Failed to fetch schema: " + response.code() + " - " + response.message();
                LOG.error(errorMsg);
                throw new IOException(errorMsg);
            }
            
            String responseBody = response.body().string();
            SchemaResponse schemaResponse = objectMapper.readValue(responseBody, SchemaResponse.class);
            
            // Find the requested schema version or use the latest one
            JsonNode schemaJson = null;
            Integer requestedVersion = schemaConfig.getSchemaVersion();
            
            if (requestedVersion != null) {
                LOG.info("Looking for specific schema version: {}", requestedVersion);
                
                for (JsonNode versionNode : schemaResponse.getSchemas().get("versions")) {
                    int version = versionNode.get("version").asInt();
                    if (version == requestedVersion) {
                        String schemaText = versionNode.get("schemaText").asText();
                        LOG.info("Found requested schema version: {}", requestedVersion);
                        return schemaText;
                    }
                }
                
                LOG.warn("Requested schema version {} not found, falling back to latest version", requestedVersion);
            }
            
            // Use the latest version (first in the array)
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

package com.dataflow.flink.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * Model class representing the response from the schema registry API
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SchemaResponse {
    private JsonNode schemaMetadata;
    private Long id;
    private Long timestamp;
    private JsonNode schemas;
    private JsonNode serDesInfos;
    
    // Default constructor
    public SchemaResponse() {}
    
    // Getters and Setters
    public JsonNode getSchemaMetadata() {
        return schemaMetadata;
    }
    
    public void setSchemaMetadata(JsonNode schemaMetadata) {
        this.schemaMetadata = schemaMetadata;
    }
    
    public Long getId() {
        return id;
    }
    
    public void setId(Long id) {
        this.id = id;
    }
    
    public Long getTimestamp() {
        return timestamp;
    }
    
    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }
    
    public JsonNode getSchemas() {
        return schemas;
    }
    
    public void setSchemas(JsonNode schemas) {
        this.schemas = schemas;
    }
    
    public JsonNode getSerDesInfos() {
        return serDesInfos;
    }
    
    public void setSerDesInfos(JsonNode serDesInfos) {
        this.serDesInfos = serDesInfos;
    }
}

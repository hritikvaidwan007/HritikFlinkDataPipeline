package com.dataflow.flink.config;

/**
 * Configuration for schema registry and validation settings
 */
public class SchemaConfig {
    private String schemaApiUrl;
    private String schemaName;
    private Integer schemaVersion;
    private boolean useLocalSchema;
    private String localSchemaPath;
    
    public SchemaConfig() {
        // Default constructor
    }
    
    public String getSchemaApiUrl() {
        return schemaApiUrl;
    }
    
    public void setSchemaApiUrl(String schemaApiUrl) {
        this.schemaApiUrl = schemaApiUrl;
    }
    
    public String getSchemaName() {
        return schemaName;
    }
    
    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }
    
    public Integer getSchemaVersion() {
        return schemaVersion;
    }
    
    public void setSchemaVersion(Integer schemaVersion) {
        this.schemaVersion = schemaVersion;
    }
    
    public boolean isUseLocalSchema() {
        return useLocalSchema;
    }
    
    public void setUseLocalSchema(boolean useLocalSchema) {
        this.useLocalSchema = useLocalSchema;
    }
    
    public String getLocalSchemaPath() {
        return localSchemaPath;
    }
    
    public void setLocalSchemaPath(String localSchemaPath) {
        this.localSchemaPath = localSchemaPath;
    }
}

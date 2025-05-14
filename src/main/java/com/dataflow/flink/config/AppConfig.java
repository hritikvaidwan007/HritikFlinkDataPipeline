package com.dataflow.flink.config;

/**
 * Main application configuration class that holds all configuration components
 */
public class AppConfig {
    private KafkaConfig kafkaConfig;
    private SchemaConfig schemaConfig;
    private String environment;
    private boolean useFileSource;
    private String inputFilePath;
    private boolean useFileSink;
    private String outputFilePath;
    
    public AppConfig() {
        // Default constructor
    }
    
    public KafkaConfig getKafkaConfig() {
        return kafkaConfig;
    }
    
    public void setKafkaConfig(KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
    }
    
    public SchemaConfig getSchemaConfig() {
        return schemaConfig;
    }
    
    public void setSchemaConfig(SchemaConfig schemaConfig) {
        this.schemaConfig = schemaConfig;
    }
    
    public String getEnvironment() {
        return environment;
    }
    
    public void setEnvironment(String environment) {
        this.environment = environment;
    }
    
    public boolean isUseFileSource() {
        return useFileSource;
    }
    
    public void setUseFileSource(boolean useFileSource) {
        this.useFileSource = useFileSource;
    }
    
    public String getInputFilePath() {
        return inputFilePath;
    }
    
    public void setInputFilePath(String inputFilePath) {
        this.inputFilePath = inputFilePath;
    }
    
    public boolean isUseFileSink() {
        return useFileSink;
    }
    
    public void setUseFileSink(boolean useFileSink) {
        this.useFileSink = useFileSink;
    }
    
    public String getOutputFilePath() {
        return outputFilePath;
    }
    
    public void setOutputFilePath(String outputFilePath) {
        this.outputFilePath = outputFilePath;
    }
}

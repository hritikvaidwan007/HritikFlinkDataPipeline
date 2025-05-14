/**
 * Configuration Loading Utility
 * 
 * This file contains the ConfigLoader class, which is responsible for loading
 * application settings from properties files and creating configuration objects.
 */
package com.dataflow.flink.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Utility class for loading application configuration from properties files.
 * 
 * Configuration is a critical aspect of Apache Flink applications, enabling:
 * - Environment-specific settings (dev, test, prod)
 * - Runtime behavior customization
 * - Connection parameters for external systems
 * - Feature toggles for different execution modes
 * 
 * In a production streaming application, configuration management:
 * - Separates code from environment-specific settings
 * - Allows non-code changes to connection parameters
 * - Supports different execution modes (e.g., local testing vs. production)
 * - Centralizes settings for easier management and documentation
 * 
 * This utility provides a clean way to load settings from properties files
 * and convert them into strongly-typed configuration objects.
 */
public class ConfigLoader {
    private static final Logger LOG = LoggerFactory.getLogger(ConfigLoader.class);
    
    /**
     * Private constructor to prevent instantiation of this utility class.
     * All methods in this class are static and should be accessed directly.
     */
    private ConfigLoader() {
        // Private constructor to prevent instantiation
    }
    
    /**
     * Loads application configuration from a properties file.
     * 
     * This method:
     * 1. Loads a properties file from classpath or filesystem
     * 2. Creates configuration objects with strongly-typed settings
     * 3. Provides sensible defaults for missing properties
     * 4. Logs the loaded configuration for transparency
     * 
     * The configuration hierarchy is structured as:
     * - AppConfig: Top-level application settings
     *   - KafkaConfig: Kafka connection and topic settings
     *   - SchemaConfig: Schema Registry and schema validation settings
     * 
     * This structured approach provides several benefits:
     * - Type safety for configuration values
     * - Logical grouping of related settings
     * - Encapsulation of configuration logic
     * - Easier testing and mocking
     *
     * @param configPath Path to the properties file (classpath or filesystem)
     * @return AppConfig object populated with values from the properties file
     * @throws IOException if the properties file cannot be read
     */
    public static AppConfig loadConfig(String configPath) throws IOException {
        LOG.info("Loading configuration from: {}", configPath);
        
        // Load properties from the specified path
        // First tries classpath, then falls back to filesystem
        Properties props = new Properties();
        try (InputStream input = ConfigLoader.class.getClassLoader().getResourceAsStream(configPath)) {
            if (input == null) {
                // Try as absolute path on the filesystem
                // This supports loading config from outside the classpath
                try (InputStream fileInput = Files.newInputStream(Paths.get(configPath))) {
                    props.load(fileInput);
                }
            } else {
                // Load from classpath resource
                props.load(input);
            }
        }
        
        // Create and populate the main application configuration
        AppConfig appConfig = new AppConfig();
        
        // Set application environment (dev, test, prod)
        // This affects logging and can be used for environment-specific behavior
        appConfig.setEnvironment(props.getProperty("app.environment", "dev"));
        
        // Set file source options for local testing mode
        // When true, reads data from a file instead of Kafka
        appConfig.setUseFileSource(Boolean.parseBoolean(props.getProperty("app.use.file.source", "false")));
        appConfig.setInputFilePath(props.getProperty("app.input.file.path", "src/main/resources/sample_data.json"));
        
        // Set file sink options for local testing mode
        // When true, writes data to a file instead of Kafka
        appConfig.setUseFileSink(Boolean.parseBoolean(props.getProperty("app.use.file.sink", "false")));
        appConfig.setOutputFilePath(props.getProperty("app.output.file.path", "output/results.json"));
        
        // Set up Kafka configuration
        // These settings control the Kafka connection and behavior
        KafkaConfig kafkaConfig = new KafkaConfig();
        
        // Basic Kafka connection settings
        kafkaConfig.setBootstrapServers(props.getProperty("kafka.bootstrap.servers", "localhost:9092"));
        kafkaConfig.setSourceTopic(props.getProperty("kafka.source.topic", "input-topic"));
        kafkaConfig.setSinkTopic(props.getProperty("kafka.sink.topic", "output-topic"));
        kafkaConfig.setConsumerGroup(props.getProperty("kafka.consumer.group", "flink-schema-validator"));
        
        // Kafka security settings (optional)
        // Used for connecting to secured Kafka clusters
        kafkaConfig.setSecurityEnabled(Boolean.parseBoolean(props.getProperty("kafka.security.enabled", "false")));
        kafkaConfig.setUsername(props.getProperty("kafka.security.username", ""));
        kafkaConfig.setPassword(props.getProperty("kafka.security.password", ""));
        kafkaConfig.setCertificatePath(props.getProperty("kafka.security.certificate.path", ""));
        
        // Dead Letter Queue (DLQ) settings
        // Controls whether invalid messages are sent to a DLQ topic
        kafkaConfig.setEnableDlq(Boolean.parseBoolean(props.getProperty("kafka.enable.dlq", "true")));
        
        // Add the Kafka config to the main app config
        appConfig.setKafkaConfig(kafkaConfig);
        
        // Set up Schema configuration
        // These settings control schema validation behavior
        SchemaConfig schemaConfig = new SchemaConfig();
        
        // Schema Registry API settings
        // Used for fetching schemas from a central registry
        schemaConfig.setSchemaApiUrl(props.getProperty("schema.api.url", 
                "https://frame-be.sandbox.analytics.yo-digital.com/api/v1/schemaregistry/schemas/TV_EVENTS_RAW.CENTRAL/aggregated?isTabular=false"));
        schemaConfig.setSchemaName(props.getProperty("schema.name", "TV_EVENTS_RAW.CENTRAL"));
        
        // Schema version setting (optional)
        // When specified, fetches a specific version of the schema
        String versionStr = props.getProperty("schema.version");
        schemaConfig.setSchemaVersion(versionStr != null ? Integer.parseInt(versionStr) : null);
        
        // Local schema file settings (for development/testing)
        // When true, loads schema from a local file instead of the API
        schemaConfig.setUseLocalSchema(Boolean.parseBoolean(props.getProperty("schema.use.local", "false")));
        schemaConfig.setLocalSchemaPath(props.getProperty("schema.local.path", "src/main/resources/sample_schema.json"));
        
        // Add the Schema config to the main app config
        appConfig.setSchemaConfig(schemaConfig);
        
        // Log the main configuration values for transparency and debugging
        LOG.info("Configuration loaded successfully: environment={}, kafka.source.topic={}, kafka.sink.topic={}", 
                appConfig.getEnvironment(), kafkaConfig.getSourceTopic(), kafkaConfig.getSinkTopic());
        
        return appConfig;
    }
}

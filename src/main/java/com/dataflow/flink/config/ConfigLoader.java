package com.dataflow.flink.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Utility class for loading application configuration from properties files
 */
public class ConfigLoader {
    private static final Logger LOG = LoggerFactory.getLogger(ConfigLoader.class);
    
    private ConfigLoader() {
        // Private constructor to prevent instantiation
    }
    
    /**
     * Loads application configuration from a properties file
     * 
     * @param configPath path to the properties file
     * @return AppConfig populated with values from the properties file
     * @throws IOException if the properties file cannot be read
     */
    public static AppConfig loadConfig(String configPath) throws IOException {
        LOG.info("Loading configuration from: {}", configPath);
        
        Properties props = new Properties();
        try (InputStream input = ConfigLoader.class.getClassLoader().getResourceAsStream(configPath)) {
            if (input == null) {
                // Try as absolute path
                try (InputStream fileInput = Files.newInputStream(Paths.get(configPath))) {
                    props.load(fileInput);
                }
            } else {
                props.load(input);
            }
        }
        
        AppConfig appConfig = new AppConfig();
        
        // Set environment
        appConfig.setEnvironment(props.getProperty("app.environment", "dev"));
        
        // Set file source options
        appConfig.setUseFileSource(Boolean.parseBoolean(props.getProperty("app.use.file.source", "false")));
        appConfig.setInputFilePath(props.getProperty("app.input.file.path", "src/main/resources/sample_data.json"));
        
        // Set up Kafka config
        KafkaConfig kafkaConfig = new KafkaConfig();
        kafkaConfig.setBootstrapServers(props.getProperty("kafka.bootstrap.servers", "localhost:9092"));
        kafkaConfig.setSourceTopic(props.getProperty("kafka.source.topic", "input-topic"));
        kafkaConfig.setSinkTopic(props.getProperty("kafka.sink.topic", "output-topic"));
        kafkaConfig.setConsumerGroup(props.getProperty("kafka.consumer.group", "flink-schema-validator"));
        kafkaConfig.setSecurityEnabled(Boolean.parseBoolean(props.getProperty("kafka.security.enabled", "false")));
        kafkaConfig.setUsername(props.getProperty("kafka.security.username", ""));
        kafkaConfig.setPassword(props.getProperty("kafka.security.password", ""));
        kafkaConfig.setCertificatePath(props.getProperty("kafka.security.certificate.path", ""));
        kafkaConfig.setEnableDlq(Boolean.parseBoolean(props.getProperty("kafka.enable.dlq", "true")));
        appConfig.setKafkaConfig(kafkaConfig);
        
        // Set up Schema config
        SchemaConfig schemaConfig = new SchemaConfig();
        schemaConfig.setSchemaApiUrl(props.getProperty("schema.api.url", 
                "https://frame-be.sandbox.analytics.yo-digital.com/api/v1/schemaregistry/schemas/TV_EVENTS_RAW.CENTRAL/aggregated?isTabular=false"));
        schemaConfig.setSchemaName(props.getProperty("schema.name", "TV_EVENTS_RAW.CENTRAL"));
        
        String versionStr = props.getProperty("schema.version");
        schemaConfig.setSchemaVersion(versionStr != null ? Integer.parseInt(versionStr) : null);
        
        schemaConfig.setUseLocalSchema(Boolean.parseBoolean(props.getProperty("schema.use.local", "false")));
        schemaConfig.setLocalSchemaPath(props.getProperty("schema.local.path", "src/main/resources/sample_schema.json"));
        appConfig.setSchemaConfig(schemaConfig);
        
        LOG.info("Configuration loaded successfully: environment={}, kafka.source.topic={}, kafka.sink.topic={}", 
                appConfig.getEnvironment(), kafkaConfig.getSourceTopic(), kafkaConfig.getSinkTopic());
        
        return appConfig;
    }
}

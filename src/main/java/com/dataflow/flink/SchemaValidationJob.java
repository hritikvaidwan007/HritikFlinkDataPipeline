/**
 * Main Apache Flink application that performs schema validation on data streams.
 * 
 * This application implements a Flink streaming job that:
 * 1. Reads data from either Kafka or a local file
 * 2. Validates the JSON messages against an Avro schema
 * 3. Routes valid and invalid messages to appropriate sinks
 * 
 * Apache Flink is a stream processing framework that provides stateful computations over 
 * unbounded and bounded data streams. Key Flink concepts used in this application:
 * 
 * - StreamExecutionEnvironment: The main entry point for building a Flink streaming job.
 * - DataStream: Represents a stream of data of a specific type.
 * - Source: The entry point for data into a Flink application (Kafka, files, etc.).
 * - Sink: The exit point for data processed by Flink (Kafka, files, etc.).
 * - Operator: Transformation applied to a DataStream (map, filter, process, etc.).
 * - Side Output: Secondary output channel used to split streams (used here for invalid messages).
 */
package com.dataflow.flink;

import com.dataflow.flink.config.AppConfig;
import com.dataflow.flink.config.ConfigLoader;
import com.dataflow.flink.config.KafkaConfig;
import com.dataflow.flink.config.SchemaConfig;
import com.dataflow.flink.model.Message;
import com.dataflow.flink.model.ValidationResult;
import com.dataflow.flink.operator.JsonToAvroTransformer;
import com.dataflow.flink.operator.SchemaValidationOperator;
import com.dataflow.flink.service.SchemaFetchService;
import com.dataflow.flink.service.SchemaValidator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.Schema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Main Flink Job class that orchestrates the schema validation pipeline.
 * This class configures and executes the Flink application, setting up sources,
 * transformations, and sinks based on configuration.
 */
public class SchemaValidationJob {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaValidationJob.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    /**
     * OutputTag used for side output collection of invalid messages.
     * In Flink, side outputs allow a single operator to emit multiple output streams.
     * Here we use it to separate valid and invalid messages after validation.
     */
    private static final OutputTag<String> INVALID_TAG = new OutputTag<String>("invalid-messages") {};

    /**
     * Main entry point for the Flink application.
     * 
     * This method:
     * 1. Loads configuration from properties file
     * 2. Sets up the Flink execution environment
     * 3. Configures data sources (Kafka or file)
     * 4. Defines the processing pipeline (schema validation)
     * 5. Configures data sinks (Kafka or console)
     * 6. Executes the Flink job
     * 
     * @param args Command line arguments
     * @throws Exception If an error occurs during job execution
     */
    public static void main(String[] args) throws Exception {
        // Parse command line parameters
        final ParameterTool params = ParameterTool.fromArgs(args);
        String configFile = params.get("config", "config/dev.properties");
        
        // Load configuration
        AppConfig appConfig = ConfigLoader.loadConfig(configFile);
        KafkaConfig kafkaConfig = appConfig.getKafkaConfig();
        SchemaConfig schemaConfig = appConfig.getSchemaConfig();
        
        // Set up execution environment
        // The StreamExecutionEnvironment is the context in which a Flink streaming program runs
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        
        // Fetch schema from API or local file
        Schema avroSchema;
        SchemaFetchService schemaService = new SchemaFetchService(schemaConfig);
        avroSchema = schemaService.fetchSchema();
        
        // Create schema validator
        SchemaValidator validator = new SchemaValidator(avroSchema);

        // Set up data source (Kafka or file)
        // Sources are where Flink reads data from - could be Kafka, files, or other systems
        DataStream<String> sourceStream;
        if (appConfig.isUseFileSource()) {
            // Use file source for local testing
            // FileSource is a bounded source that reads records from files
            FileSource<String> fileSource = FileSource
                    .forRecordStreamFormat(new TextLineInputFormat(), new Path(appConfig.getInputFilePath()))
                    .build();
            // WatermarkStrategy.noWatermarks() means we're not using event time processing
            sourceStream = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "File Source");
            LOG.info("Using file source: {}", appConfig.getInputFilePath());
        } else {
            // Use Kafka source for production
            // KafkaSource is an unbounded source that reads records from Kafka topics
            KafkaSource<String> kafkaSource = createKafkaSource(kafkaConfig);
            sourceStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");
            LOG.info("Using Kafka source: {}", kafkaConfig.getSourceTopic());
        }

        // Parse JSON and validate against schema
        // This creates a transformation pipeline with multiple operations
        SingleOutputStreamOperator<ValidationResult> validationResults = sourceStream
                // Map operation transforms each element in the stream
                .map(new MapFunction<String, JsonNode>() {
                    @Override
                    public JsonNode map(String value) throws Exception {
                        try {
                            return objectMapper.readTree(value);
                        } catch (Exception e) {
                            LOG.error("Failed to parse JSON: {}", value, e);
                            ObjectNode errorNode = objectMapper.createObjectNode();
                            errorNode.put("error", "Invalid JSON format");
                            errorNode.put("raw_message", value);
                            return errorNode;
                        }
                    }
                })
                // Process operation is a more complex transformation that can use state and side outputs
                .process(new SchemaValidationOperator(validator, INVALID_TAG));

        // Get invalid messages as a side output
        // Side outputs allow producing multiple output streams from a single operator
        DataStream<String> invalidMessages = validationResults.getSideOutput(INVALID_TAG);

        // Create Kafka sinks
        // Sinks define where Flink writes its results to
        if (!appConfig.isUseFileSource()) {
            // Sink for valid messages
            KafkaSink<String> validSink = createKafkaSink(kafkaConfig, kafkaConfig.getSinkTopic());
            
            // Convert ValidationResult to JSON string for valid messages
            // This chain of operations filters valid results and transforms them before sending to Kafka
            validationResults
                .filter(result -> result.isValid())
                .map(result -> objectMapper.writeValueAsString(result.getData()))
                .sinkTo(validSink);
            
            // Sink for invalid messages (DLQ) if enabled
            // DLQ (Dead Letter Queue) is a pattern for handling messages that fail processing
            if (kafkaConfig.isEnableDlq()) {
                String dlqTopic = kafkaConfig.getSourceTopic() + "_dlq";
                KafkaSink<String> invalidSink = createKafkaSink(kafkaConfig, dlqTopic);
                invalidMessages.sinkTo(invalidSink);
                LOG.info("DLQ is enabled, invalid messages will be sent to: {}", dlqTopic);
            } else {
                LOG.info("DLQ is disabled, invalid messages will be discarded");
            }
        } else {
            // Print results to stdout for local testing
            // These operations add a prefix to each message and print to console
            validationResults
                .filter(result -> result.isValid())
                .map(result -> "VALID: " + objectMapper.writeValueAsString(result.getData()))
                .print();
            
            invalidMessages
                .map(msg -> "INVALID: " + msg)
                .print();
        }

        // Execute the Flink job
        // This triggers the actual execution of the Flink pipeline
        env.execute("Kafka Schema Validation Job");
    }

    /**
     * Creates a Kafka source connector for consuming data from Kafka.
     * 
     * A KafkaSource in Flink is a connector that reads data from Kafka topics.
     * This method configures:
     * - Basic connection properties (bootstrap servers, consumer group)
     * - Security settings (SASL/SSL if enabled)
     * - Deserialization (how to convert Kafka records to Java objects)
     * - Offset initialization (where to start reading from the topic)
     * 
     * @param config Kafka configuration containing connection details
     * @return Configured KafkaSource that can be used in a Flink job
     */
    private static KafkaSource<String> createKafkaSource(KafkaConfig config) {
        // Set basic Kafka consumer properties
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", config.getBootstrapServers());
        props.setProperty("group.id", config.getConsumerGroup());
        
        // Add security configuration if needed
        // Kafka supports various security protocols including SASL and SSL
        if (config.isSecurityEnabled()) {
            props.setProperty("security.protocol", SecurityProtocol.SASL_SSL.name);
            props.setProperty("sasl.mechanism", "PLAIN");
            props.setProperty("sasl.jaas.config", 
                    "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + 
                    config.getUsername() + "\" password=\"" + config.getPassword() + "\";");
            
            if (config.getCertificatePath() != null && !config.getCertificatePath().isEmpty()) {
                props.setProperty("ssl.truststore.location", config.getCertificatePath());
            }
        }
        
        // Build the KafkaSource with the configured properties
        return KafkaSource.<String>builder()
                .setProperties(props)
                .setTopics(config.getSourceTopic())
                // OffsetsInitializer.earliest() means start reading from the beginning of the topic
                .setStartingOffsets(OffsetsInitializer.earliest())
                // SimpleStringSchema deserializes Kafka records as strings
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }

    /**
     * Creates a Kafka sink connector for producing data to Kafka.
     * 
     * A KafkaSink in Flink is a connector that writes data to Kafka topics.
     * This method configures:
     * - Basic connection properties (bootstrap servers)
     * - Producer settings (idempotence, acks, retries)
     * - Security settings (SASL/SSL if enabled)
     * - Serialization (how to convert Java objects to Kafka records)
     * - Delivery guarantee (at-least-once ensures no data loss)
     * 
     * @param config Kafka configuration containing connection details
     * @param topic Target Kafka topic to write messages to
     * @return Configured KafkaSink that can be used in a Flink job
     */
    private static KafkaSink<String> createKafkaSink(KafkaConfig config, String topic) {
        // Set up producer properties
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", config.getBootstrapServers());
        // Enable idempotence to prevent duplicate messages
        props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        // Configure 'acks=all' for strongest durability guarantee
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        // Set retries to handle transient failures
        props.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        
        // Add security configuration if needed
        if (config.isSecurityEnabled()) {
            props.setProperty("security.protocol", SecurityProtocol.SASL_SSL.name);
            props.setProperty("sasl.mechanism", "PLAIN");
            props.setProperty("sasl.jaas.config", 
                    "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + 
                    config.getUsername() + "\" password=\"" + config.getPassword() + "\";");
            
            if (config.getCertificatePath() != null && !config.getCertificatePath().isEmpty()) {
                props.setProperty("ssl.truststore.location", config.getCertificatePath());
            }
        }
        
        // Build the KafkaSink with the configured properties
        return KafkaSink.<String>builder()
                .setKafkaProducerConfig(props)
                // Configure serialization for the sink
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                // AT_LEAST_ONCE delivery guarantee ensures no data loss
                // (may produce duplicates in failure scenarios)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }
}

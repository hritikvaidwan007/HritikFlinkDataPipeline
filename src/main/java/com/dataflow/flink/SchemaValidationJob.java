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

public class SchemaValidationJob {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaValidationJob.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final OutputTag<String> INVALID_TAG = new OutputTag<String>("invalid-messages") {};

    public static void main(String[] args) throws Exception {
        // Parse command line parameters
        final ParameterTool params = ParameterTool.fromArgs(args);
        String configFile = params.get("config", "config/dev.properties");
        
        // Load configuration
        AppConfig appConfig = ConfigLoader.loadConfig(configFile);
        KafkaConfig kafkaConfig = appConfig.getKafkaConfig();
        SchemaConfig schemaConfig = appConfig.getSchemaConfig();
        
        // Set up execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        
        // Fetch schema from API or local file
        Schema avroSchema;
        SchemaFetchService schemaService = new SchemaFetchService(schemaConfig);
        avroSchema = schemaService.fetchSchema();
        
        // Create schema validator
        SchemaValidator validator = new SchemaValidator(avroSchema);

        // Set up data source (Kafka or file)
        DataStream<String> sourceStream;
        if (appConfig.isUseFileSource()) {
            // Use file source for local testing
            FileSource<String> fileSource = FileSource
                    .forRecordStreamFormat(new TextLineInputFormat(), new Path(appConfig.getInputFilePath()))
                    .build();
            sourceStream = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "File Source");
            LOG.info("Using file source: {}", appConfig.getInputFilePath());
        } else {
            // Use Kafka source for production
            KafkaSource<String> kafkaSource = createKafkaSource(kafkaConfig);
            sourceStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");
            LOG.info("Using Kafka source: {}", kafkaConfig.getSourceTopic());
        }

        // Parse JSON and validate against schema
        SingleOutputStreamOperator<ValidationResult> validationResults = sourceStream
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
                .process(new SchemaValidationOperator(validator, INVALID_TAG));

        // Get invalid messages as a side output
        DataStream<String> invalidMessages = validationResults.getSideOutput(INVALID_TAG);

        // Create Kafka sinks
        if (!appConfig.isUseFileSource()) {
            // Sink for valid messages
            KafkaSink<String> validSink = createKafkaSink(kafkaConfig, kafkaConfig.getSinkTopic());
            
            // Convert ValidationResult to JSON string for valid messages
            validationResults
                .filter(result -> result.isValid())
                .map(result -> objectMapper.writeValueAsString(result.getData()))
                .sinkTo(validSink);
            
            // Sink for invalid messages (DLQ) if enabled
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
            validationResults
                .filter(result -> result.isValid())
                .map(result -> "VALID: " + objectMapper.writeValueAsString(result.getData()))
                .print();
            
            invalidMessages
                .map(msg -> "INVALID: " + msg)
                .print();
        }

        // Execute the Flink job
        env.execute("Kafka Schema Validation Job");
    }

    private static KafkaSource<String> createKafkaSource(KafkaConfig config) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", config.getBootstrapServers());
        props.setProperty("group.id", config.getConsumerGroup());
        
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
        
        return KafkaSource.<String>builder()
                .setProperties(props)
                .setTopics(config.getSourceTopic())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }

    private static KafkaSink<String> createKafkaSink(KafkaConfig config, String topic) {
        // Set up producer properties
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", config.getBootstrapServers());
        props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
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
        
        return KafkaSink.<String>builder()
                .setKafkaProducerConfig(props)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }
}

package com.dataflow.flink.operator;

import com.dataflow.flink.utils.AvroUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flink MapFunction that transforms JSON data to Avro GenericRecord
 */
public class JsonToAvroTransformer extends RichMapFunction<JsonNode, GenericRecord> {
    private static final Logger LOG = LoggerFactory.getLogger(JsonToAvroTransformer.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    private final Schema schema;
    
    public JsonToAvroTransformer(Schema schema) {
        this.schema = schema;
    }
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        LOG.info("JsonToAvroTransformer initialized with schema: {}", schema.getName());
    }
    
    @Override
    public GenericRecord map(JsonNode value) throws Exception {
        try {
            return AvroUtil.convertJsonToAvro(schema, value);
        } catch (Exception e) {
            LOG.error("Error transforming JSON to Avro: {}", e.getMessage(), e);
            throw e;
        }
    }
}

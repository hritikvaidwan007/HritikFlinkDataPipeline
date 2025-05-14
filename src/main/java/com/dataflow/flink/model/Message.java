package com.dataflow.flink.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * Model class representing a message with metadata
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Message {
    private JsonNode data;
    private String topic;
    private Integer partition;
    private Long offset;
    private Long timestamp;
    
    // Default constructor
    public Message() {}
    
    // Constructor with data
    public Message(JsonNode data) {
        this.data = data;
    }
    
    // Getters and Setters
    public JsonNode getData() {
        return data;
    }
    
    public void setData(JsonNode data) {
        this.data = data;
    }
    
    public String getTopic() {
        return topic;
    }
    
    public void setTopic(String topic) {
        this.topic = topic;
    }
    
    public Integer getPartition() {
        return partition;
    }
    
    public void setPartition(Integer partition) {
        this.partition = partition;
    }
    
    public Long getOffset() {
        return offset;
    }
    
    public void setOffset(Long offset) {
        this.offset = offset;
    }
    
    public Long getTimestamp() {
        return timestamp;
    }
    
    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }
    
    @Override
    public String toString() {
        return "Message{" +
               "topic='" + topic + '\'' +
               ", partition=" + partition +
               ", offset=" + offset +
               ", timestamp=" + timestamp +
               ", data=" + data +
               '}';
    }
}

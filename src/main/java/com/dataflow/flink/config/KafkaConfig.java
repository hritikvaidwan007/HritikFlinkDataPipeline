package com.dataflow.flink.config;

/**
 * Configuration for Kafka source and sink properties
 */
public class KafkaConfig {
    private String bootstrapServers;
    private String sourceTopic;
    private String sinkTopic;
    private String consumerGroup;
    private boolean securityEnabled;
    private String username;
    private String password;
    private String certificatePath;
    private boolean enableDlq;
    
    public KafkaConfig() {
        // Default constructor
    }
    
    public String getBootstrapServers() {
        return bootstrapServers;
    }
    
    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }
    
    public String getSourceTopic() {
        return sourceTopic;
    }
    
    public void setSourceTopic(String sourceTopic) {
        this.sourceTopic = sourceTopic;
    }
    
    public String getSinkTopic() {
        return sinkTopic;
    }
    
    public void setSinkTopic(String sinkTopic) {
        this.sinkTopic = sinkTopic;
    }
    
    public String getConsumerGroup() {
        return consumerGroup;
    }
    
    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }
    
    public boolean isSecurityEnabled() {
        return securityEnabled;
    }
    
    public void setSecurityEnabled(boolean securityEnabled) {
        this.securityEnabled = securityEnabled;
    }
    
    public String getUsername() {
        return username;
    }
    
    public void setUsername(String username) {
        this.username = username;
    }
    
    public String getPassword() {
        return password;
    }
    
    public void setPassword(String password) {
        this.password = password;
    }
    
    public String getCertificatePath() {
        return certificatePath;
    }
    
    public void setCertificatePath(String certificatePath) {
        this.certificatePath = certificatePath;
    }
    
    public boolean isEnableDlq() {
        return enableDlq;
    }
    
    public void setEnableDlq(boolean enableDlq) {
        this.enableDlq = enableDlq;
    }
}

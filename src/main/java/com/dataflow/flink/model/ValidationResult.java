package com.dataflow.flink.model;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Model class representing the result of schema validation
 */
public class ValidationResult {
    private final boolean valid;
    private final JsonNode data;
    private final String errorMessage;
    
    public ValidationResult(boolean valid, JsonNode data, String errorMessage) {
        this.valid = valid;
        this.data = data;
        this.errorMessage = errorMessage;
    }
    
    public boolean isValid() {
        return valid;
    }
    
    public JsonNode getData() {
        return data;
    }
    
    public String getErrorMessage() {
        return errorMessage;
    }
    
    @Override
    public String toString() {
        return "ValidationResult{" +
               "valid=" + valid +
               ", errorMessage='" + errorMessage + '\'' +
               ", data=" + data +
               '}';
    }
}

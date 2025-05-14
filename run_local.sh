#!/bin/bash

# ========================================================
# Flink Schema Validation Application - Local Execution Script
# ========================================================
#
# This script builds and runs the Flink schema validation application
# in local mode for development and testing purposes.
#
# The script:
# 1. Builds the application JAR file using Maven
# 2. Executes the application in local mode with specific configuration
#
# Key aspects:
# - Uses Maven exec plugin for local execution
# - Uses local.properties for configuration
# - Skips tests for faster iteration
# - Does not require a Flink cluster or Kafka

# Build the application with Maven
# - clean: Removes previous build artifacts
# - package: Compiles and packages the application
# - DskipTests: Skips running tests for faster builds
echo "Building Flink schema validation application..."
mvn clean package -DskipTests

# Run the application using Maven exec plugin
# This executes the application directly from Maven without deploying to a cluster
# The --config argument specifies the configuration file to use
echo "Running application with Maven exec plugin..."
echo "Using configuration: config/local.properties"
mvn exec:java -Dexec.mainClass="com.dataflow.flink.SchemaValidationJob" -Dexec.args="--config config/local.properties"

# Notes:
# - For production deployment, use the deployment guide instead of this script
# - This script is intended for local development and testing only
# - The local.properties file should have app.use.file.source=true for local testing
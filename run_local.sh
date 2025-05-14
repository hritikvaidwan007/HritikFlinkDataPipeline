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
# - Runs the application directly from the shaded JAR file
# - Uses local.properties for configuration
# - Skips tests for faster iteration
# - Does not require a Flink cluster or Kafka

# Build the application with Maven
# - clean: Removes previous build artifacts
# - package: Compiles and packages the application into a JAR with all dependencies
# - DskipTests: Skips running tests for faster builds
echo "Building Flink schema validation application..."
mvn clean package -DskipTests

# Check if the build was successful
if [ ! -f "target/flink-schema-validator-1.0.0.jar" ]; then
    echo "Error: Build failed or JAR file not found."
    exit 1
fi

# Run the application directly from the JAR file
# This executes the application using the JVM without needing a Flink cluster
# The --config argument specifies the configuration file to use
echo "Running application from JAR file..."
echo "Using configuration: config/local.properties"
java -jar target/flink-schema-validator-1.0.0.jar --config config/local.properties

# Notes:
# - For production deployment, use the deployment guide instead of this script
# - This script is intended for local development and testing only
# - The local.properties file should have app.use.file.source=true for local testing
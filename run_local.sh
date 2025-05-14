#!/bin/bash

# Build the application
mvn clean package -DskipTests

# Run the application with Maven exec plugin
echo "Running application with Maven exec plugin"
mvn exec:java -Dexec.args="--config config/local.properties"
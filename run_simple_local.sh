#!/bin/bash

# Ensure output directory exists
mkdir -p output

# Compile the project
mvn clean compile

# Run the SimpleLocalRunner
mvn exec:java -Dexec.mainClass="com.dataflow.flink.SimpleLocalRunner"
#!/bin/bash

# ========================================================
# Flink Schema Validation Test Local Sink Script
# ========================================================
#
# This script compiles and runs the TestLocalSink class directly
# to demonstrate the file sink functionality.
#
# The script:
# 1. Compiles the project
# 2. Runs the TestLocalSink class to create sample output files
#

# Compile the project
echo "Compiling project..."
mvn clean compile

# Check if compilation was successful
if [ $? -ne 0 ]; then
    echo "Error: Compilation failed."
    exit 1
fi

# Run the TestLocalSink class
echo "Running TestLocalSink to demonstrate file sink functionality..."
mvn exec:java -Dexec.mainClass="com.dataflow.flink.TestLocalSink"

# Check output files
echo ""
echo "Checking output files:"
ls -la output/

# Display content of output files
echo ""
echo "Content of valid records file (output/validation_results.json):"
cat output/validation_results.json

echo ""
echo "Content of invalid records file (output/validation_results.json.invalid):"
cat output/validation_results.json.invalid

echo ""
echo "Test completed."
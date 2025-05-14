#!/bin/bash

# ========================================================
# Flink Schema Validation - File Sink Example Runner
# ========================================================
#
# This script runs the LocalFileSinkExample class which demonstrates
# the file sink functionality with actual schema validation.
#
# The script:
# 1. Compiles the project
# 2. Runs LocalFileSinkExample to process sample data
# 3. Displays the validation results
#

echo "===== Local File Sink Example ====="
echo "This example demonstrates the local file sink functionality with real schema validation"
echo "using the sample data from config/sample_data.json."
echo ""

# Compile the project
echo "Step 1: Compiling project..."
mvn compile

# Check if compilation was successful
if [ $? -ne 0 ]; then
    echo "Error: Compilation failed."
    exit 1
fi

# Run the example
echo ""
echo "Step 2: Running LocalFileSinkExample..."
mvn exec:java -Dexec.mainClass="com.dataflow.flink.LocalFileSinkExample"

# Display results
echo ""
echo "Step 3: Checking validation results..."
ls -la output/

echo ""
echo "Valid records (passed schema validation):"
echo "------------------------------------------------------"
cat output/validation_results.json 2>/dev/null || echo "No valid records found"

echo ""
echo "Invalid records (failed schema validation):"
echo "------------------------------------------------------"
cat output/validation_results.json.invalid 2>/dev/null || echo "No invalid records found"

echo ""
echo "===== Example Complete ====="
echo ""
echo "This demonstrates how your Flink application can:"
echo "1. Read input data from files or Kafka"
echo "2. Validate each record against an Avro schema"
echo "3. Write valid and invalid records to separate files"
echo "4. Provide detailed error messages for invalid records"
echo ""
echo "This functionality makes local development and testing much easier"
echo "by eliminating the need for a Kafka cluster during development."
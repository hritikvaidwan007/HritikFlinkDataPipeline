#!/bin/bash

# ========================================================
# Flink Schema Validation - File Sink Demo
# ========================================================
#
# This script demonstrates the local file sink functionality
# by running the SimpleDemoWriter class.
#
# The script:
# 1. Compiles the project
# 2. Runs SimpleDemoWriter to create example output files
# 3. Displays the output files and their content
#

echo "===== File Sink Demonstration ====="
echo "This demo shows how the Flink application can write validation results to local files"
echo "instead of Kafka topics for easier local development and testing."
echo ""

# Compile the project
echo "Step 1: Compiling project..."
mvn clean compile

# Check if compilation was successful
if [ $? -ne 0 ]; then
    echo "Error: Compilation failed."
    exit 1
fi

# Create output directory
mkdir -p output

# Run the SimpleDemoWriter class
echo ""
echo "Step 2: Running file sink demo..."
java -cp target/classes com.dataflow.flink.SimpleDemoWriter

# Display results
echo ""
echo "Step 3: Checking output files..."
ls -la output/

echo ""
echo "Valid records file content (validation_results.json):"
echo "------------------------------------------------------"
cat output/validation_results.json

echo ""
echo "Invalid records file content (validation_results.json.invalid):"
echo "------------------------------------------------------"
cat output/validation_results.json.invalid

echo ""
echo "===== File Sink Demonstration Complete ====="
echo ""
echo "In a real Flink application:"
echo "1. Valid records would be written to this format in the regular output file"
echo "2. Invalid records with schema validation errors would be written to the .invalid file"
echo "3. This provides an easy way to inspect validation results during development"
echo ""
echo "To use this feature in your application, set in your properties file:"
echo "app.use.file.sink=true"
echo "app.output.file.path=output/validation_results.json"
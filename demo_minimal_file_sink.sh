#!/bin/bash

# ========================================================
# Minimal File Sink Demo
# ========================================================
#
# This script demonstrates the file sink functionality
# using the most minimal possible example.
#

echo "Compiling application..."
mvn compile

echo -e "\nRunning minimal file sink demonstration..."
mvn exec:java -Dexec.mainClass="com.dataflow.flink.FileSinkOnly"

echo -e "\n===================================="
echo "This example shows:"
echo "1. How to create output directories"
echo "2. How to write valid records to a .json file" 
echo "3. How to write invalid records to a .json.invalid file"
echo "4. Basic error handling"
echo -e "====================================\n"

echo "To view the output files again, run:"
echo "cat output/validation_results.json"
echo "cat output/validation_results.json.invalid"
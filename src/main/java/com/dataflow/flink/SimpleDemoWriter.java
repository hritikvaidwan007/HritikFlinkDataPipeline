package com.dataflow.flink;

import java.io.*;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Simple demonstration class for the local file sink functionality.
 * This creates example output files without using any complex dependencies.
 */
public class SimpleDemoWriter {
    public static void main(String[] args) {
        try {
            System.out.println("Starting Simple Demo Writer for File Sink functionality...");
            
            // Create output directory
            String outputDir = "output";
            Files.createDirectories(Paths.get(outputDir));
            
            // Define output files
            String validOutputFile = outputDir + "/validation_results.json";
            String invalidOutputFile = validOutputFile + ".invalid";
            
            System.out.println("Creating output files:");
            System.out.println("- Valid records: " + validOutputFile);
            System.out.println("- Invalid records: " + invalidOutputFile);
            
            // Remove any existing files
            Files.deleteIfExists(Paths.get(validOutputFile));
            Files.deleteIfExists(Paths.get(invalidOutputFile));
            
            // Create sample data
            List<String> validRecords = new ArrayList<>();
            validRecords.add("{\"_timestamp\": \"2023-07-01T10:15:30Z\", \"title\": \"Valid Event\"}");
            validRecords.add("{\"_timestamp\": \"2023-07-01T10:16:45Z\", \"title\": \"Another Valid Event\"}");
            
            List<String> invalidRecords = new ArrayList<>();
            invalidRecords.add("Invalid schema: Field 'schemaId' is not of type Integer");
            
            // Write the files
            Files.write(Paths.get(validOutputFile), validRecords);
            Files.write(Paths.get(invalidOutputFile), invalidRecords);
            
            System.out.println("Files created successfully!");
            System.out.println("File sink demonstration completed.");
            
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
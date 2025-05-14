package com.dataflow.flink;

import java.io.*;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Extremely simple demonstration of file sink functionality.
 * This class only shows the file writing capability, with no dependencies
 * on other parts of the system.
 */
public class FileSinkOnly {
    public static void main(String[] args) {
        try {
            System.out.println("====================================");
            System.out.println("File Sink Functionality Demonstration");
            System.out.println("====================================");
            
            // Define output paths
            String outputDir = "output";
            String validFilePath = outputDir + "/validation_results.json";
            String invalidFilePath = validFilePath + ".invalid";
            
            System.out.println("Using output directory: " + outputDir);
            System.out.println("Valid records file: " + validFilePath);
            System.out.println("Invalid records file: " + invalidFilePath);
            
            // Create directory if it doesn't exist
            Files.createDirectories(Paths.get(outputDir));
            
            // Delete existing files
            Files.deleteIfExists(Paths.get(validFilePath));
            Files.deleteIfExists(Paths.get(invalidFilePath));
            
            // Create sample data
            List<String> validRecords = new ArrayList<>();
            validRecords.add("{\"id\": 1, \"message\": \"This is a valid record\"}");
            validRecords.add("{\"id\": 2, \"message\": \"This is another valid record\"}");
            
            List<String> invalidRecords = new ArrayList<>();
            invalidRecords.add("Invalid record: Missing required field 'id'");
            invalidRecords.add("Invalid record: Field 'count' is not of type Integer");
            
            // Write to files
            Files.write(Paths.get(validFilePath), validRecords);
            Files.write(Paths.get(invalidFilePath), invalidRecords);
            
            System.out.println("Files created successfully!");
            System.out.println("\nValid records file content:");
            System.out.println("---------------------------");
            for (String line : Files.readAllLines(Paths.get(validFilePath))) {
                System.out.println(line);
            }
            
            System.out.println("\nInvalid records file content:");
            System.out.println("---------------------------");
            for (String line : Files.readAllLines(Paths.get(invalidFilePath))) {
                System.out.println(line);
            }
            
            System.out.println("\nDemo complete!");
            System.out.println("====================================");
            
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
import java.io.*;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

public class test_local_sink {
    public static void main(String[] args) {
        try {
            System.out.println("Testing local file sink...");
            
            // Create output directories
            String outputPath = "output/validation_results.json";
            String invalidPath = outputPath + ".invalid";
            
            Files.createDirectories(Paths.get(outputPath).getParent());
            
            // Delete existing files
            Files.deleteIfExists(Paths.get(outputPath));
            Files.deleteIfExists(Paths.get(invalidPath));
            
            // Sample data
            List<String> validData = new ArrayList<>();
            validData.add("{\"_timestamp\": \"2023-07-01T10:15:30Z\", \"title\": \"Valid Event\"}");
            validData.add("{\"_timestamp\": \"2023-07-01T10:16:45Z\", \"title\": \"Another Valid Event\"}");
            
            List<String> invalidData = new ArrayList<>();
            invalidData.add("Invalid schema: Field 'schemaId' is not of type Integer");
            
            // Write to files
            Files.write(Paths.get(outputPath), validData);
            Files.write(Paths.get(invalidPath), invalidData);
            
            System.out.println("Files written successfully!");
            System.out.println("Valid data: " + outputPath);
            System.out.println("Invalid data: " + invalidPath);
            
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}

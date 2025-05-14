# Local File Sink Feature Guide

## Overview

This document explains the Local File Sink feature added to the Flink Schema Validation application. This feature allows the validation results to be written directly to files in local development mode instead of sending them to Kafka topics.

## Configuration

The local file sink is configured in the `AppConfig` and `ConfigLoader` classes with the following properties:

- `app.use.file.sink`: Boolean flag to enable/disable file sink (true/false)
- `app.output.file.path`: Path where output files will be created

Example configuration in `local.properties`:
```properties
# Local file sink configuration
app.use.file.sink=true
app.output.file.path=output/validation_results.json
```

## Output Files

When the file sink is enabled, the application creates two types of files:

1. **Valid Records**: `{outputPath}` - Contains valid records in JSON format
2. **Invalid Records**: `{outputPath}.invalid` - Contains invalid records with error messages

Example:
- `output/validation_results.json` - Valid messages
- `output/validation_results.json.invalid` - Invalid messages with validation errors

## Usage Scenarios

### Local Development and Testing

The file sink is particularly useful for local development and testing:

1. It eliminates the need for a running Kafka cluster
2. It allows direct inspection of validation results in the filesystem
3. It simplifies debugging of schema validation issues

### Example Output

**Valid records file** (`output/validation_results.json`):
```json
{"_timestamp": "2023-07-01T10:15:30Z", "title": "Valid Event"}
{"_timestamp": "2023-07-01T10:16:45Z", "title": "Another Valid Event"}
```

**Invalid records file** (`output/validation_results.json.invalid`):
```
Invalid schema: Field 'schemaId' is not of type Integer
```

## Implementation Classes

The feature is implemented across several classes:

1. **SchemaValidationJob**: Main class that configures the sink based on application config
2. **AppConfig**: Contains the file sink configuration properties
3. **ConfigLoader**: Loads file sink settings from properties files
4. **SimpleLocalRunner**: Simplified runner for local testing without Flink runtime
5. **TestLocalSink**: Test class that demonstrates the file sink functionality

## Local Testing Without Flink

To test the file sink functionality without starting a Flink cluster, use:

```bash
mvn compile exec:java -Dexec.mainClass="com.dataflow.flink.SimpleDemoWriter"
```

This will create example output files with sample valid and invalid records.
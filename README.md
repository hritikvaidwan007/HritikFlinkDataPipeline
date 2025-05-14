# Flink Schema Validation Job

A production-ready Apache Flink application that consumes data from Kafka, validates messages against Avro schema fetched from an API, and produces valid messages to another Kafka topic.

## Features

- **Kafka Integration**: Consume from and produce to Kafka topics
- **Schema Validation**: Validate JSON messages against Avro schema fetched from API
- **Configuration-driven**: All components are configurable through property files
- **Multi-environment Support**: Different configurations for local, dev, and production environments
- **Dead Letter Queue**: Invalid messages can be sent to DLQ topics or discarded
- **Local Development Mode**: Test with file-based input/output and local schema for easier debugging
- **Error Handling**: Comprehensive error handling and reporting
- **Logging**: Detailed logging with statistics reporting

## Architecture

```
+----------------+     +---------------------+     +--------------------+
|                |     |                     |     |                    |
|  Kafka Source  +---->+  Schema Validation  +---->+   Kafka Sink      |
|                |     |                     |     |                    |
+----------------+     +----------+----------+     +--------------------+
                                  |
                                  |
                                  v
                        +--------------------+
                        |                    |
                        |  DLQ (Invalid)     |
                        |                    |
                        +--------------------+

```

## Prerequisites

- Java 11+
- Apache Maven 3.6+
- Apache Flink 1.16.1+
- Apache Kafka cluster (for production use)

## Configuration

The application is fully configurable through property files located in `config/`:

- `local.properties`: For local development with file source
- `dev.properties`: For development environment with Kafka
- `prod.properties`: For production environment with Kafka

### Key Configuration Properties

#### Application Settings
- `app.environment`: Environment name (local, dev, prod)
- `app.use.file.source`: Whether to use file as input source instead of Kafka
- `app.input.file.path`: Path to input JSON file when using file source
- `app.use.file.sink`: Whether to use file as output sink instead of Kafka
- `app.output.file.path`: Path to output file when using file sink

#### Kafka Settings
- `kafka.bootstrap.servers`: Comma-separated list of Kafka brokers
- `kafka.source.topic`: Topic to consume messages from
- `kafka.sink.topic`: Topic to produce valid messages to
- `kafka.consumer.group`: Consumer group ID
- `kafka.security.enabled`: Whether Kafka security is enabled
- `kafka.security.username`: Username for Kafka authentication
- `kafka.security.password`: Password for Kafka authentication
- `kafka.security.certificate.path`: Path to Kafka certificate file
- `kafka.enable.dlq`: Whether to send invalid messages to DLQ topic

#### Schema Settings
- `schema.use.local`: Whether to use local schema file
- `schema.local.path`: Path to local schema file
- `schema.api.url`: URL of schema registry API
- `schema.name`: Schema name
- `schema.version`: Schema version (optional, defaults to latest)

## Building and Running

### Local Development

Build and run the application in local mode:

```bash
# Build the project
mvn clean package

# Option 1: Run the full application with local configuration
java -cp target/flink-schema-validator-1.0.0-shaded.jar com.dataflow.flink.SchemaValidationJob --config config/local.properties

# Option 2: Run the simplified local file sink example
./run_file_sink_example.sh
```

#### Local File Sink

The application supports writing validation results to local files instead of Kafka topics. This is useful for local development and testing without a Kafka cluster.

When `app.use.file.sink=true` is set in the properties file:
- Valid records are written to the file specified by `app.output.file.path`
- Invalid records are written to `app.output.file.path + ".invalid"`

Example output:
```
# Valid records (validation_results.json)
{"_timestamp": "2023-07-01T10:15:30Z", "title": "Sample Event", "schemaId": 7}

# Invalid records (validation_results.json.invalid)
Invalid schema: Field 'schemaId' is not of type Integer
```

### Deploying to AWS EMR

1. Upload the JAR file to an S3 bucket:
```bash
aws s3 cp target/flink-schema-validator-1.0.0-shaded.jar s3://your-bucket/jars/
```

2. Create an EMR cluster with Flink installed:
```bash
aws emr create-cluster \
    --name "Flink Cluster" \
    --release-label emr-6.5.0 \
    --applications Name=Flink \
    --ec2-attributes KeyName=your-key \
    --instance-type m5.xlarge \
    --instance-count 3 \
    --use-default-roles
```

3. Submit the Flink job to the EMR cluster:
```bash
aws emr add-steps \
    --cluster-id <cluster-id> \
    --steps Type=CUSTOM_JAR,Name="Flink Schema Validation Job",Jar="command-runner.jar",\
Args=["flink","run","-m","yarn-cluster","-ynm","SchemaValidationJob","-c","com.dataflow.flink.SchemaValidationJob","s3://your-bucket/jars/flink-schema-validator-1.0.0-shaded.jar","--config","s3://your-bucket/config/prod.properties"]
```

### Deploying to EMR on EKS

1. Create an EMR on EKS virtual cluster:
```bash
aws emr-containers create-virtual-cluster \
    --name flink-cluster \
    --container-provider '{
        "id": "your-eks-cluster-name",
        "type": "EKS",
        "info": {
            "eksInfo": {
                "namespace": "flink"
            }
        }
    }'
```

2. Create a job run:
```bash
aws emr-containers start-job-run \
    --virtual-cluster-id <virtual-cluster-id> \
    --name schema-validation-job \
    --execution-role-arn <execution-role-arn> \
    --release-label emr-6.5.0-flink-1.16.1 \
    --job-driver '{
        "flinkRunJobDriver": {
            "entryPoint": "s3://your-bucket/jars/flink-schema-validator-1.0.0-shaded.jar",
            "entryPointArguments": ["--config", "s3://your-bucket/config/prod.properties"],
            "mainClass": "com.dataflow.flink.SchemaValidationJob"
        }
    }' \
    --configuration-overrides '{
        "applicationConfiguration": [
            {
                "classification": "flink-conf.yaml",
                "properties": {
                    "taskmanager.numberOfTaskSlots": "4",
                    "parallelism.default": "4"
                }
            }
        ],
        "monitoringConfiguration": {
            "cloudWatchMonitoringConfiguration": {
                "logGroupName": "/emr-containers/flink",
                "logStreamNamePrefix": "schema-validation-job"
            },
            "s3MonitoringConfiguration": {
                "logUri": "s3://your-bucket/logs/"
            }
        }
    }'
```

## Running Tests

```bash
mvn test
```

## License

This project is licensed under the Apache License 2.0.
# Deployment Guide for Flink Schema Validation Job

This guide provides detailed instructions for deploying the Flink Schema Validation Job to AWS EMR and EMR on EKS.

## Prerequisites

- AWS CLI configured with appropriate IAM permissions
- An S3 bucket for storing application JAR files, configs, and logs
- VPC with appropriate subnets for EMR or an existing EKS cluster for EMR on EKS

## Step 1: Build the Application

```bash
# Build the package and skip tests
mvn clean package -DskipTests

# Verify the JAR was created
ls -la target/flink-schema-validator-1.0.0-shaded.jar
```

## Step 2: Prepare Configuration Files

Customize the production configuration file (`config/prod.properties`) according to your environment:

```properties
# Production environment configuration
app.environment=prod
app.use.file.source=false

# Kafka configuration
kafka.bootstrap.servers=${KAFKA_BROKERS}
kafka.source.topic=${KAFKA_SOURCE_TOPIC}
kafka.sink.topic=${KAFKA_SINK_TOPIC}
kafka.consumer.group=flink-schema-validator-prod
kafka.security.enabled=true
kafka.security.username=${KAFKA_USERNAME}
kafka.security.password=${KAFKA_PASSWORD}
kafka.security.certificate.path=${KAFKA_CERT_PATH}
kafka.enable.dlq=true

# Schema configuration
schema.use.local=false
schema.api.url=${SCHEMA_API_URL}
schema.name=${SCHEMA_NAME}
schema.version=${SCHEMA_VERSION}
```

## Step 3: Upload Files to S3

```bash
# Create S3 directories
aws s3 mb s3://your-bucket/flink-schema-validator/
aws s3 mb s3://your-bucket/flink-schema-validator/jars
aws s3 mb s3://your-bucket/flink-schema-validator/config
aws s3 mb s3://your-bucket/flink-schema-validator/logs

# Upload JAR and config
aws s3 cp target/flink-schema-validator-1.0.0-shaded.jar s3://your-bucket/flink-schema-validator/jars/
aws s3 cp config/prod.properties s3://your-bucket/flink-schema-validator/config/
```

## Step 4: Option A - Deploy to AWS EMR

### Create an EMR Cluster

```bash
aws emr create-cluster \
    --name "Flink Schema Validator" \
    --release-label emr-6.5.0 \
    --applications Name=Flink \
    --ec2-attributes KeyName=your-key-pair,SubnetId=subnet-xxxxxxxx \
    --instance-type m5.xlarge \
    --instance-count 3 \
    --use-default-roles \
    --log-uri s3://your-bucket/flink-schema-validator/logs/ \
    --bootstrap-actions Path=s3://your-bucket/flink-schema-validator/bootstrap.sh
```

### Submit the Flink Job

```bash
aws emr add-steps \
    --cluster-id <cluster-id> \
    --steps Type=CUSTOM_JAR,Name="Schema Validation Job",Jar="command-runner.jar",\
Args=["flink","run","-m","yarn-cluster","-ynm","SchemaValidationJob","-c","com.dataflow.flink.SchemaValidationJob", \
"s3://your-bucket/flink-schema-validator/jars/flink-schema-validator-1.0.0-shaded.jar","--config","s3://your-bucket/flink-schema-validator/config/prod.properties"]
```

## Step 4: Option B - Deploy to EMR on EKS

### Step 4.1: Create a Virtual Cluster (if not already created)

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

### Step 4.2: Create Necessary IAM Roles

```bash
# Create execution role for EMR on EKS
aws iam create-role --role-name EMRContainers-JobExecutionRole --assume-role-policy-document '{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "emr-containers.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}'

# Attach policies
aws iam attach-role-policy \
  --role-name EMRContainers-JobExecutionRole \
  --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess

aws iam attach-role-policy \
  --role-name EMRContainers-JobExecutionRole \
  --policy-arn arn:aws:iam::aws:policy/CloudWatchLogsFullAccess
```

### Step 4.3: Start Job Run

```bash
aws emr-containers start-job-run \
    --virtual-cluster-id <virtual-cluster-id> \
    --name schema-validation-job \
    --execution-role-arn arn:aws:iam::<account-id>:role/EMRContainers-JobExecutionRole \
    --release-label emr-6.5.0-flink-1.16.1 \
    --job-driver '{
        "flinkRunJobDriver": {
            "entryPoint": "s3://your-bucket/flink-schema-validator/jars/flink-schema-validator-1.0.0-shaded.jar",
            "entryPointArguments": ["--config", "s3://your-bucket/flink-schema-validator/config/prod.properties"],
            "mainClass": "com.dataflow.flink.SchemaValidationJob"
        }
    }' \
    --configuration-overrides '{
        "applicationConfiguration": [
            {
                "classification": "flink-conf.yaml",
                "properties": {
                    "taskmanager.numberOfTaskSlots": "4",
                    "parallelism.default": "4",
                    "execution.checkpointing.interval": "60000",
                    "execution.checkpointing.mode": "EXACTLY_ONCE",
                    "state.backend": "filesystem",
                    "state.checkpoints.dir": "s3://your-bucket/flink-schema-validator/checkpoints"
                }
            }
        ],
        "monitoringConfiguration": {
            "cloudWatchMonitoringConfiguration": {
                "logGroupName": "/emr-containers/flink",
                "logStreamNamePrefix": "schema-validation-job"
            },
            "s3MonitoringConfiguration": {
                "logUri": "s3://your-bucket/flink-schema-validator/logs/"
            }
        }
    }'
```

## Step 5: Monitoring and Scaling

### Monitoring with EMR

```bash
# Get cluster status
aws emr describe-cluster --cluster-id <cluster-id>

# List steps
aws emr list-steps --cluster-id <cluster-id>

# View step details
aws emr describe-step --cluster-id <cluster-id> --step-id <step-id>
```

### Monitoring with EMR on EKS

```bash
# List job runs for virtual cluster
aws emr-containers list-job-runs --virtual-cluster-id <virtual-cluster-id>

# Get job run details
aws emr-containers describe-job-run \
    --virtual-cluster-id <virtual-cluster-id> \
    --id <job-run-id>
```

## Step 6: Cleanup and Termination

### Terminating EMR Cluster

```bash
# Terminate EMR cluster
aws emr terminate-clusters --cluster-ids <cluster-id>
```

### Terminating EMR on EKS Jobs and Clusters

```bash
# Cancel a running job
aws emr-containers cancel-job-run \
    --virtual-cluster-id <virtual-cluster-id> \
    --id <job-run-id>

# Delete virtual cluster
aws emr-containers delete-virtual-cluster \
    --id <virtual-cluster-id>
```

## Automated Deployment with CI/CD

For automated deployments, consider setting up a CI/CD pipeline with AWS CodePipeline, GitHub Actions, or Jenkins.

### Sample GitHub Actions Workflow

```yaml
name: Deploy Flink Schema Validator

on:
  push:
    branches: [ main ]
  workflow_dispatch:

jobs:
  build-and-deploy:
    runs-on: ubuntu-latest
    
    steps:
    - uses: actions/checkout@v2
    
    - name: Set up JDK 11
      uses: actions/setup-java@v2
      with:
        java-version: '11'
        distribution: 'adopt'
    
    - name: Build with Maven
      run: mvn clean package -DskipTests
    
    - name: Configure AWS credentials
      uses: aws-actions/configure-aws-credentials@v1
      with:
        aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
        aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
        aws-region: us-east-1
    
    - name: Upload to S3
      run: |
        aws s3 cp target/flink-schema-validator-1.0.0-shaded.jar s3://your-bucket/flink-schema-validator/jars/
        aws s3 cp config/prod.properties s3://your-bucket/flink-schema-validator/config/
    
    - name: Submit EMR on EKS job
      run: |
        aws emr-containers start-job-run \
          --virtual-cluster-id ${{ secrets.VIRTUAL_CLUSTER_ID }} \
          --name schema-validation-job \
          --execution-role-arn ${{ secrets.EXECUTION_ROLE_ARN }} \
          --release-label emr-6.5.0-flink-1.16.1 \
          --job-driver '{
              "flinkRunJobDriver": {
                  "entryPoint": "s3://your-bucket/flink-schema-validator/jars/flink-schema-validator-1.0.0-shaded.jar",
                  "entryPointArguments": ["--config", "s3://your-bucket/flink-schema-validator/config/prod.properties"],
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
                      "logUri": "s3://your-bucket/flink-schema-validator/logs/"
                  }
              }
          }'
```
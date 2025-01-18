#!/usr/bin/env bash
set -euo pipefail

# Variables - replace these with your actual values
SCRIPTS_BUCKET="mentorhub-training-emr-scripts-bucket"
CLUSTER_ID="j-XCSRAY1X7ULD"
REGION="us-east-1"
S3_SCRIPTS_PREFIX="scripts"

# Local paths to code
LOCAL_PYSPARK_SCRIPT="ingestions.py"
LOCAL_PYSPARK_SCRIPT2="ingestions.py"
LOCAL_REQUIREMENTS="requirements.txt"

# Upload code and requirements to S3
echo "Uploading PySpark script to S3..."
aws s3 cp "${LOCAL_PYSPARK_SCRIPT}" "s3://${SCRIPTS_BUCKET}/${S3_SCRIPTS_PREFIX}/ingestions.py" --region "${REGION}"

aws s3 cp "${LOCAL_PYSPARK_SCRIPT2}" "s3://${SCRIPTS_BUCKET}/${S3_SCRIPTS_PREFIX}/enrichment.py" --region "${REGION}"

echo "Uploading requirements.txt to S3 (if needed)..."
aws s3 cp "${LOCAL_REQUIREMENTS}" "s3://${SCRIPTS_BUCKET}/${S3_SCRIPTS_PREFIX}/requirements.txt" --region "${REGION}"

# Now add a step to the EMR cluster to run the ingestions.py script.
# This assumes you've already run the bootstrap action to install requirements.txt previously,
# or the cluster environment is already prepared. If not, you'd need to handle that separately.

echo "Adding Spark step to EMR cluster..."
STEP_ID=$(aws emr add-steps \
    --cluster-id "${CLUSTER_ID}" \
    --steps "[
      {
        \"Name\": \"Run Ingestions Job\",
        \"ActionOnFailure\": \"CONTINUE\",
        \"Type\": \"CUSTOM_JAR\",
        \"Jar\": \"command-runner.jar\",
        \"Args\": [
          \"spark-submit\",
          \"--master\", \"yarn\",
          \"--deploy-mode\", \"cluster\",
          \"s3://${SCRIPTS_BUCKET}/${S3_SCRIPTS_PREFIX}/enrichment.py\"
        ]
      }
    ]" \
    --query 'StepIds[0]' \
    --output text \
    --region "${REGION}")

echo "Step submitted with ID: ${STEP_ID}"
echo "You can check the EMR console or use 'aws emr describe-step --cluster-id ${CLUSTER_ID} --step-id ${STEP_ID}' to monitor progress."
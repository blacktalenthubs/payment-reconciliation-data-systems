#!/usr/bin/env bash
# file: upload_and_run_emr_batch.sh
#
# Usage:
#   ./upload_and_run_emr_batch.sh <EMR_CLUSTER_ID> <LOCAL_SCRIPT_PATH> <S3_DEST_PATH>
#
# Example:
#   ./upload_and_run_emr_batch.sh j-3XXXXXX ./batch_index.py s3://my-bucket/scripts/batch_index.py
#
# 1) Uploads your local Python script to the specified S3 path
# 2) Adds an EMR spark-submit step to run that script on the specified cluster

set -e

if [ $# -lt 3 ]; then
  echo "Usage: $0 <EMR_CLUSTER_ID> <LOCAL_SCRIPT_PATH> <S3_DEST_PATH>"
  exit 1
fi

CLUSTER_ID="$1"
LOCAL_SCRIPT="$2"
S3_SCRIPT="$3"

# 1) Upload local script to S3
echo "Uploading $LOCAL_SCRIPT to $S3_SCRIPT ..."
aws s3 cp "$LOCAL_SCRIPT" "$S3_SCRIPT"

# 2) Run spark-submit step on EMR
echo "Adding spark-submit step to EMR cluster $CLUSTER_ID with $S3_SCRIPT ..."
aws emr add-steps \
  --cluster-id "$CLUSTER_ID" \
  --steps Type=Spark,Name=BatchStep,ActionOnFailure=CONTINUE,\
Args=["spark-submit","--deploy-mode","cluster","$S3_SCRIPT"]

echo "Batch step submitted to cluster $CLUSTER_ID for $S3_SCRIPT"
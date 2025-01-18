#!/usr/bin/env bash

# file: build_and_push_kafka_cli.sh

set -e

AWS_REGION="us-east-1"
ACCOUNT_ID="123456789012"  # change to your own AWS account
REPO_NAME="msk"      # your ECR repo name
IMAGE_TAG="latest"

# 1) Create ECR repository (skip if already exists)
aws ecr describe-repositories --repository-names "${REPO_NAME}" 2>/dev/null || \
aws ecr create-repository --repository-name "${REPO_NAME}"

# 2) Docker build
docker build -t "${REPO_NAME}:${IMAGE_TAG}" -f Dockerfile .

# 3) ECR login
aws ecr get-login-password --region "${AWS_REGION}" \
  | docker login --username AWS --password-stdin "${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"

# 4) Tag & push
docker tag "${REPO_NAME}:${IMAGE_TAG}" "${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${REPO_NAME}:${IMAGE_TAG}"
docker push "${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${REPO_NAME}:${IMAGE_TAG}"

echo "Successfully built and pushed image: ${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${REPO_NAME}:${IMAGE_TAG}"
#!/usr/bin/env bash

set -e


AWS_REGION="us-east-1"
ECR_REPO="689688817484.dkr.ecr.us-east-1.amazonaws.com/kafka-cli"
IMAGE_NAME="mentorhub-streaming"
IMAGE_TAG="latest"
DOCKERFILE="Dockerfile"

# 1) Authenticate Docker to AWS ECR
echo "Logging in to ECR..."
aws ecr get-login-password --region "$AWS_REGION" \
  | docker login --username AWS --password-stdin "$ECR_REPO"

# 2) Build Docker image
echo "Building Docker image for streaming..."
docker build -t "$IMAGE_NAME:$IMAGE_TAG" -f "$DOCKERFILE" .

# 3) Tag image for ECR
echo "Tagging image..."
docker tag "$IMAGE_NAME:$IMAGE_TAG" "$ECR_REPO:$IMAGE_TAG"

# 4) Push to ECR
echo "Pushing image to ECR..."
docker push "$ECR_REPO:$IMAGE_TAG"

echo "Successfully built and pushed streaming image to $ECR_REPO:$IMAGE_TAG"
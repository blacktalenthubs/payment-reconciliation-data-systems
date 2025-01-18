#!/usr/bin/env bash

set -e



AWS_REGION="us-east-1"
ECR_REPO="689688817484.dkr.ecr.us-east-1.amazonaws.com/batch_build"
IMAGE_NAME="mentorhub-batch"
IMAGE_TAG="latest"
DOCKERFILE="Dockerfile"

echo "Logging in to ECR..."
aws ecr get-login-password --region "$AWS_REGION" \
  | docker login --username AWS --password-stdin "$ECR_REPO"

echo "Building Docker image for batch..."
docker build -t "$IMAGE_NAME:$IMAGE_TAG" -f "$DOCKERFILE" .

echo "Tagging image..."
docker tag "$IMAGE_NAME:$IMAGE_TAG" "$ECR_REPO:$IMAGE_TAG"

echo "Pushing image to ECR..."
docker push "$ECR_REPO:$IMAGE_TAG"

echo "Successfully built and pushed batch image to $ECR_REPO:$IMAGE_TAG"
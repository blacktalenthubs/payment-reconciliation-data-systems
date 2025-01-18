
Below is a plan for **deploying a continuous Spark streaming job** with **Spinnaker**, rather than Airflow or other orchestrators. Spinnaker is generally used for **continuous delivery** of services or applications, and it can also manage long-running containerized Spark streaming jobs on Kubernetes or ECS.

---

# 1) High-Level Architecture

1. **Docker-ized Spark Streaming App**  
   - Container image that runs a Spark Structured Streaming job, continuously reading from Kafka (or MSK) and writing results to S3 or another sink.  
2. **Kubernetes (EKS) or ECS**  
   - Cluster where the streaming container runs.  
   - Alternatively, if you prefer EMR on EKS for Spark, the container must include Spark + dependencies and you run it in Kubernetes-mode Spark.  
3. **Spinnaker**  
   - A continuous delivery (CD) platform that monitors your Docker registry (e.g., ECR).  
   - Triggers a pipeline when a new container image is pushed.  
   - Deploys or updates a Kubernetes/ECS service/pod that runs your streaming job.  
4. **Kafka**  
   - Source of real-time events.  
5. **S3** (or another sink)  
   - Destination of streaming output in Parquet, or noSQL store if your architecture demands it.

---

# 2) Containerizing the Streaming Job

Below is an **example Dockerfile** that packages your Spark streaming code (the “streaming_index.py” or similar). This example uses a standalone Spark install (for small scale) or you can adapt it for Spark on Kubernetes (Spark driver/executors launched in K8s pods).

```dockerfile
# Dockerfile for streaming Spark job
FROM openjdk:11-jre-slim
LABEL maintainer="you@example.com"

# Install needed OS packages
RUN apt-get update && apt-get install -y python3 python3-pip wget curl

# Install Spark (lightweight example)
ENV SPARK_VERSION=3.3.1
ENV HADOOP_VERSION=3
RUN wget -qO - "https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz" \
    | tar -xz -C /opt/
ENV SPARK_HOME=/opt/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION
ENV PATH="$SPARK_HOME/bin:$PATH"

# Install Python dependencies
WORKDIR /app
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy your Spark streaming code
COPY streaming_index.py /app/streaming_index.py

# If you have schemas or other .py modules, copy them too
COPY schemas.py /app/schemas.py

# Example entrypoint to run streaming job directly
CMD ["spark-submit", "/app/streaming_index.py"]
```

1. **Build & Push** to ECR (or your Docker registry):
   ```bash
   docker build -t my-streaming-job:latest .
   docker tag my-streaming-job:latest <AWS_ACCOUNT_ID>.dkr.ecr.us-east-1.amazonaws.com/my-streaming-job:latest
   docker push <AWS_ACCOUNT_ID>.dkr.ecr.us-east-1.amazonaws.com/my-streaming-job:latest
   ```

2. In your `streaming_index.py`, make sure it’s **self-contained**:  
   - Reads from Kafka topic.  
   - Writes to S3 (or anywhere).  
   - Runs **indefinitely** (structured streaming does not exit unless there’s an error).

---

# 3) Kubernetes or ECS Deployment

## Option A: **EKS (Kubernetes)**

1. **K8s Deployment** YAML:
   ```yaml
   apiVersion: apps/v1
   kind: Deployment
   metadata:
     name: spark-streaming-job
   spec:
     replicas: 1
     selector:
       matchLabels:
         app: spark-streaming
     template:
       metadata:
         labels:
           app: spark-streaming
       spec:
         containers:
         - name: spark-streaming-container
           image: "<AWS_ACCOUNT_ID>.dkr.ecr.us-east-1.amazonaws.com/my-streaming-job:latest"
           imagePullPolicy: Always
           env:
             - name: KAFKA_BOOTSTRAP
               value: "kafka-broker:9092"
             - name: S3_OUTPUT_PATH
               value: "s3://my-stream-output/"
           # Additional memory/cpu requests if needed
           resources:
             requests:
               memory: "2Gi"
               cpu: "1000m"
             limits:
               memory: "4Gi"
               cpu: "2000m"
   ```

   This defines a single **long-running** Spark streaming container. If you are launching a real Spark cluster on K8s (driver + executors), you’d do `spark-submit --master k8s://...`, but let’s keep it simpler by running everything in one container for the demonstration.

2. You’ll store this YAML in a git repository or artifact store so Spinnaker can deploy it.

## Option B: **ECS**  
Define an ECS **Task Definition** referencing your container image, with 1 CPU + 2GB memory (or more). Then define a **Service** with **desiredCount=1** (one task), so it runs continuously.

---

# 4) Spinnaker Pipeline

Spinnaker can watch for a new Docker image in ECR. Once a new image is pushed, it triggers a pipeline that **deploys** the updated image to Kubernetes or ECS. Below is a conceptual pipeline:

1. **Trigger**: Docker Registry trigger (pointing to `<AWS_ACCOUNT_ID>.dkr.ecr.us-east-1.amazonaws.com/my-streaming-job`).  
2. **Bake** or “Find Image” Stage: Spinnaker identifies the new container tag.  
3. **Deploy** (Kubernetes or ECS) Stage: Spinnaker updates the existing `Deployment` or ECS service with the new container version.  

### Example Spinnaker Pipeline JSON Snippet (Kubernetes)

If you store your K8s manifest in a Git repo, Spinnaker can deploy it. For instance:

```json
{
  "application": "spark-stream",
  "name": "DeployStreamingJob",
  "stages": [
    {
      "type": "findImageFromTags",
      "name": "Find Docker Image",
      "account": "my-docker-registry",
      "tags": ["latest"],
      "cloudProvider": "docker",
      "requisiteStageRefIds": []
    },
    {
      "type": "deployManifest",
      "name": "Deploy to EKS",
      "account": "my-k8s-account",
      "moniker": {
        "app": "spark-stream",
        "stack": "prod"
      },
      "source": "git/repo",
      "manifestArtifactId": "some-manifest-reference",
      "requisiteStageRefIds": ["Find Docker Image"]
    }
  ]
}
```

- **Find Docker Image**: Finds the newly pushed ECR image with `:latest`.  
- **DeployManifest**: Applies or updates the K8s `Deployment` YAML, substituting the new image tag.  

In the UI, you’d create a pipeline with these stages, or use Halyard to manage Spinnaker config. The key is that when you push a new Docker image to ECR, the pipeline picks it up and redeploys your streaming job with minimal downtime (maybe a rolling update if specified).

---

# 5) No Airflow Involvement

This method avoids Airflow entirely:

- Your **batch** jobs might still be orchestrated by Spinnaker or run on a schedule in ECS/EC2.  
- Your **streaming** job is a **long-lived** container or set of containers.  
- Spinnaker automatically redeploys each time you produce a new container image, ensuring continuous delivery.

---

# 6) Additional Considerations

1. **Stateful vs. Stateless**  
   - A typical Spark streaming job is stateless if you’re doing simple transformations. If you do stateful aggregations, you need persistent checkpoint storage (S3, HDFS, etc.). Make sure your container references the same checkpoint path in S3 so restarts don’t cause data reprocessing.

2. **Autoscaling**  
   - If you expect spikes in event volume, you can scale up the number of streaming containers (or Spark executors).  
   - This can be integrated with Kubernetes Horizontal Pod Autoscaler (HPA) if your application is built to scale horizontally.  
   - However, a single container or single driver typically limits concurrency, so advanced Spark on K8s modes might be necessary for real elasticity.

3. **Authentication**  
   - For pulling from ECR, ensure your EKS or ECS cluster can pull private images.  
   - For writing to S3, you’ll have an IAM role assigned to the K8s service account or ECS task role.

4. **Monitoring & Logging**  
   - Use CloudWatch, Datadog, or Prometheus to gather container logs and Spark metrics.  
   - You want to confirm the streaming job is healthy, reading from Kafka offsets, and not failing silently.

5. **Cost**  
   - One or more continuously running containers mean continuous cost. Evaluate cluster sizing or ephemeral approaches.

---

# 7) Putting It All Together: Deployment Workflow

1. **Local Dev & Docker Build**  
   - Write your Spark streaming code (`streaming_index.py`).  
   - Build Docker with Spark + Python.  
   - Test locally using docker run or a local K8s environment (like minikube).

2. **Push to ECR**  
   - Tag and push the Docker image to your ECR repo.

3. **Spinnaker Pipeline**  
   - Spinnaker sees the new Docker image tag.  
   - Proceeds to “Deploy Manifest” stage on your EKS cluster, referencing your `Deployment` or `StatefulSet` YAML.  
   - The job is updated with the new image.

4. **Continuous Running**  
   - The Spark streaming job reads Kafka data indefinitely.  
   - Writes output to S3 or a NoSQL store.

5. **Observability**  
   - Check logs in CloudWatch (if using a sidecar or fluentd to push logs).  
   - Check the Spark UI if exposed.  
   - Confirm partition offsets in Kafka are advancing as expected.

---

## Final Summary

Using **Spinnaker** to deploy a **long-running Spark streaming job** involves:

1. **Containerizing** the Spark streaming code.  
2. **Defining** a Kubernetes (or ECS) **deployment** for your streaming job.  
3. **Creating** a **Spinnaker pipeline** that triggers on new Docker images and updates the streaming deployment.  
4. The container remains running indefinitely, reading Kafka topics, writing data to your S3 or data store.  

This approach eliminates the need for Airflow in streaming orchestrations. Spinnaker becomes your **continuous delivery** solution, ensuring each new version of the streaming job is automatically deployed with minimal downtime.
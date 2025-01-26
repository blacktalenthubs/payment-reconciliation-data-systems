
- A **local Docker registry** (for storing images).  
- **Minikube** for a simple Kubernetes cluster on their workstation.  
- A **local Airflow** instance (running via Docker Compose or similar) that uses the **KubernetesPodOperator** to schedule tasks on Minikube.  

The overall flow:

1. **Build** and **push** images to a **local Docker registry** (`localhost:5000`).  
2. **Deploy** or reference those images in **Minikube**.  
3. **Airflow** runs locally and creates Pods in Minikube with the **KubernetesPodOperator**, pulling from the local registry.

This approach avoids any external cloud services so students can learn the end-to-end pipeline on a single machine.

---

# **1. Launch a Local Docker Registry**

1. Open a terminal and run:

   ```bash
   docker run -d -p 5000:5000 --name local-registry registry:2
   ```

   - This starts a container named `local-registry` hosting a registry on `localhost:5000`.

2. Verify it’s up by opening `http://localhost:5000/v2/_catalog` in a browser (it may be empty at first).

---

# **2. Start Minikube**

1. Make sure you have **Minikube** installed.  
2. Run:

   ```bash
   minikube start
   ```

   - This starts a local Kubernetes cluster.

3. Configure Docker to use Minikube’s Docker daemon (optional):
   ```bash
   eval $(minikube docker-env)
   ```
   - This means **Docker builds** go directly into Minikube’s internal Docker, so you might not even need to push to a local registry. However, to teach the “push” concept, we’ll still walk through the local registry approach.

---

# **3. Example Build-and-Push Script**

Below is a **shell script** that **builds** the Docker images for our example pipelines (the “streaming” code, the “batch” code) and pushes them to `localhost:5000`. Customize it for your actual files and image names.

**File: `build_and_push_images.sh`**:

```bash
#!/usr/bin/env bash
# build_and_push_images.sh
# Builds Docker images and pushes them to local registry at localhost:5000.

set -e  # stop on error

# 1) Build streaming pipeline image
echo "Building streaming pipeline image..."
docker build -t streaming-pipeline:local -f Dockerfile.streaming .
echo "Tagging and pushing to localhost:5000/streaming-pipeline:local..."
docker tag streaming-pipeline:local localhost:5000/streaming-pipeline:local
docker push localhost:5000/streaming-pipeline:local

# 2) Build batch pipeline image
echo "Building batch pipeline image..."
docker build -t batch-jobs:local -f Dockerfile.batch .
echo "Tagging and pushing to localhost:5000/batch-jobs:local..."
docker tag batch-jobs:local localhost:5000/batch-jobs:local
docker push localhost:5000/batch-jobs:local

echo "All images pushed successfully!"
```

### **Usage**

1. Make sure the local registry container is running (`docker ps` to confirm).  
2. Run:
   ```bash
   chmod +x build_and_push_images.sh
   ./build_and_push_images.sh
   ```
3. Check `http://localhost:5000/v2/_catalog` again; you should now see `streaming-pipeline` and `batch-jobs` in the registry.

---

# **4. Create or Update Kubernetes Manifests**

If you want to **continuously run** the streaming pipeline on Minikube, you can use a **Deployment** referencing `localhost:5000/streaming-pipeline:local`. For example:

**`streaming_pipeline_deployment.yaml`**:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: streaming-pipeline-deployment
spec:
  replicas: 1
  selector:
    matchLabels:
      app: streaming-pipeline
  template:
    metadata:
      labels:
        app: streaming-pipeline
    spec:
      containers:
      - name: streaming-pipeline-container
        image: localhost:5000/streaming-pipeline:local
        imagePullPolicy: IfNotPresent
        env:
        - name: KAFKA_BOOTSTRAP_SERVERS
          value: "some-local-kafka:9092"
        - name: KAFKA_TOPIC
          value: "transactions_topic"
```

Then:

```bash
kubectl apply -f streaming_pipeline_deployment.yaml
```

Now your streaming pipeline runs continuously inside Minikube, pulling the image from the local registry.

---

# **5. Local Airflow Setup**

We’ll run Airflow in **Docker Compose**—**not** inside Minikube—so that the **KubernetesPodOperator** can connect to Minikube’s API.  
- Alternatively, you can run Airflow *in* Minikube, but running it externally is more straightforward for a classroom environment.

### **5.1 Docker Compose for Airflow**

Create a minimal **`docker-compose.yaml`**:

```yaml
version: '3'
services:
  airflow-webserver:
    image: apache/airflow:2.5.1
    environment:
      - AIRFLOW__CORE__EXECUTOR=LocalExecutor
      - AIRFLOW__CORE__FERNET_KEY=YOUR_FERNET_KEY
      - AIRFLOW__CORE__LOAD_EXAMPLES=False
      - AIRFLOW__WEBSERVER__ENABLE_PROXY_FIX=True
    volumes:
      - ./dags:/opt/airflow/dags
    ports:
      - "8080:8080"
    command: >
      bash -c "
      airflow db upgrade &&
      airflow users create --username admin --password admin --firstname First --lastname Last --role Admin --email admin@example.com &&
      airflow webserver
      "
    depends_on:
      - airflow-scheduler

  airflow-scheduler:
    image: apache/airflow:2.5.1
    environment:
      - AIRFLOW__CORE__EXECUTOR=LocalExecutor
      - AIRFLOW__CORE__FERNET_KEY=YOUR_FERNET_KEY
      - AIRFLOW__CORE__LOAD_EXAMPLES=False
    volumes:
      - ./dags:/opt/airflow/dags
    command: >
      bash -c "
      airflow db upgrade &&
      airflow scheduler
      "

  airflow-init:
    image: apache/airflow:2.5.1
    command: airflow db init
    depends_on:
      - airflow-scheduler
      - airflow-webserver
```

1. **`./dags`**: A folder in the same directory as the compose file containing your DAG code (below).  
2. Start Airflow with:
   ```bash
   docker-compose up -d
   ```
3. Access the Airflow UI at `http://localhost:8080/` (user: `admin`, pass: `admin` per the example).

---

## **6. Example Airflow DAG using KubernetesPodOperator**

Below is a minimal DAG that:

1. **Generates synthetic data** using an image from your local registry: `localhost:5000/batch-jobs:local` (assuming it has the `end_to_end_data_setup.py`).  
2. **Runs the batch reconciliation** job.  
3. **Runs the final index** job.

Each step runs as a **KubernetesPodOperator** creating a Pod in **Minikube**. The images are pulled from the local registry `localhost:5000`. Make sure Airflow can connect to Minikube’s API (see note below).

**`dags/local_pipeline_dag.py`**:

```python
from datetime import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

default_args = {
    "owner": "student",
    "start_date": datetime(2025, 1, 1),
    "retries": 0
}

dag = DAG(
    "local_pipeline_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

# Task 1: Synthetic data ingestion
ingestion_task = KubernetesPodOperator(
    task_id="synthetic_data_ingestion",
    namespace="default",
    name="synthetic-data-ingestion-pod",
    image="localhost:5000/batch-jobs:local",
    cmds=["python", "end_to_end_data_setup.py"],
    image_pull_policy="IfNotPresent",
    get_logs=True,
    dag=dag
)

# Task 2: Batch Reconciliation
batch_recon_task = KubernetesPodOperator(
    task_id="batch_reconciliation",
    namespace="default",
    name="batch-reconciliation-pod",
    image="localhost:5000/batch-jobs:local",
    cmds=["python", "batch.py"],
    image_pull_policy="IfNotPresent",
    get_logs=True,
    dag=dag
)

# Task 3: Final Index
final_index_task = KubernetesPodOperator(
    task_id="final_index",
    namespace="default",
    name="final-index-pod",
    image="localhost:5000/batch-jobs:local",
    cmds=["python", "final_index_pipeline.py"],
    image_pull_policy="IfNotPresent",
    get_logs=True,
    dag=dag
)

ingestion_task >> batch_recon_task >> final_index_task
```

### **Important**: Airflow -> Minikube Connection

- The **KubernetesPodOperator** needs to know how to talk to the **Kubernetes API** in Minikube. Typically, you set:

  ```bash
  export KUBE_CONFIG_PATH=~/.kube/config
  ```

  Then in your **airflow.cfg** or environment variables for the Docker containers, you might point `AIRFLOW__KUBERNETES__CONFIG_FILE=/home/airflow/.kube/config` or use an **in-cluster** config if Airflow was also in the cluster.  

- Alternatively, you can **mount** your local `~/.kube/config` into the Airflow container, so that `KubernetesPodOperator` can read it. This can be done via volumes in `docker-compose.yaml` (though it’s a bit advanced for a student environment).

**Local Tip**: If you run Airflow on your host machine (not inside Docker Compose), it can read your `~/.kube/config` directly. Then the `KubernetesPodOperator` can easily connect to Minikube.  

---

## **7. Summary of Steps for Students**

1. **Start Local Registry**  
   ```bash
   docker run -d -p 5000:5000 --name local-registry registry:2
   ```
2. **Build & Push Images**  
   ```bash
   ./build_and_push_images.sh
   ```
   - This tags them as `localhost:5000/xxx:local` and pushes to your registry.

3. **Start Minikube**  
   ```bash
   minikube start
   ```
4. **(Optional) Deploy streaming pipeline**  
   ```bash
   kubectl apply -f streaming_pipeline_deployment.yaml
   ```
   - This runs continuously to read from Kafka.

5. **Start Airflow**  
   ```bash
   docker-compose up -d
   ```
   - Airflow web is at `http://localhost:8080/`.

6. **Configure Airflow** to talk to Minikube:  
   - Ensure `KUBECONFIG` is accessible. Possibly mount `~/.kube/config` into the container or run Airflow outside Docker on your host with the same environment.  

7. **Open Airflow UI** and **trigger** `local_pipeline_dag`.  
   - You should see tasks succeed, each creating ephemeral Pods in Minikube, pulling from `localhost:5000`.

---

## **Conclusion**

By following these steps:

- **Students** learn how to **containerize** code (streaming, batch) with Docker.  
- They **push** images to a **local registry**.  
- They run **Minikube** for a local Kubernetes environment.  
- **Airflow** (running locally) uses the **KubernetesPodOperator** to spin up pods in Minikube, pulling images from the local registry.  
- The streaming pipeline is **deployed** as a **Deployment** in Minikube, continuously running.  
- The **batch DAG** is triggered manually or on a schedule, coordinating the data ingestion script, batch reconciliation, and final index pipelines.


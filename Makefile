# ==============================================================================
# Distributed Data Engines — Automation Makefile
# ==============================================================================

.PHONY: help build-mr test-mr run-spark docker-build docker-up docker-down k8s-deploy k8s-status k8s-scale k8s-clean

help:
	@echo "Distributed Data Engines Build & Orchestration Suite"
	@echo "----------------------------------------------------"
	@echo "Hadoop MapReduce (Java):"
	@echo "  make build-mr       - Compile and package MapReduce JAR via Maven"
	@echo "  make test-mr        - Run unit tests with MRUnit and JUnit"
	@echo ""
	@echo "Apache Spark (Python):"
	@echo "  make run-spark      - Run PySpark wordcount and log telemetry pipelines"
	@echo ""
	@echo "Docker Distributed Simulation:"
	@echo "  make docker-build   - Build multi-stage Hadoop/Spark runtime image"
	@echo "  make docker-up      - Start 2-node virtual cluster (master + worker)"
	@echo "  make docker-down    - Tear down local virtual cluster"
	@echo ""
	@echo "Kubernetes Microservices (DockerCoins):"
	@echo "  make k8s-deploy     - Deploy 5-tier microservice architecture to K8s"
	@echo "  make k8s-status     - Check pods, services, and deployments"
	@echo "  make k8s-scale N=4  - Scale worker daemon to N replicas"
	@echo "  make k8s-clean      - Tear down all DockerCoins K8s resources"

# ------------------------------------------------------------------------------
# Hadoop MapReduce Targets
# ------------------------------------------------------------------------------
build-mr:
	@echo "[+] Compiling MapReduce design patterns package..."
	cd mapreduce-patterns && mvn clean package -DskipTests

test-mr:
	@echo "[+] Running MRUnit & JUnit tests..."
	cd mapreduce-patterns && mvn test

# ------------------------------------------------------------------------------
# Apache Spark Targets
# ------------------------------------------------------------------------------
run-spark:
	@echo "[+] Executing Spark WordCount pipeline..."
	python3 spark-telemetry/word_count.py
	@echo "[+] Executing NASA Common Log Format analytics pipeline..."
	python3 spark-telemetry/log_analysis.py

# ------------------------------------------------------------------------------
# Docker Runtime Targets
# ------------------------------------------------------------------------------
docker-build:
	@echo "[+] Building Docker runtime image..."
	cd cluster-deployments/docker-runtime && docker compose build

docker-up:
	@echo "[+] Bootstrapping multi-node bridge cluster..."
	cd cluster-deployments/docker-runtime && docker compose up -d
	@echo "[+] Cluster running: Master at http://localhost:8088 (YARN) and http://localhost:50070 (HDFS)"

docker-down:
	@echo "[+] Tearing down Docker cluster..."
	cd cluster-deployments/docker-runtime && docker compose down

# ------------------------------------------------------------------------------
# Kubernetes Orchestration Targets
# ------------------------------------------------------------------------------
k8s-deploy:
	@echo "[+] Applying declarative Kubernetes manifests..."
	kubectl apply -f cluster-deployments/kubernetes-microservices/
	@echo "[+] Web UI exposed at NodePort 30001 (or run: kubectl port-forward svc/webui 8000:80)"

k8s-status:
	@kubectl get pods,svc,deployments -l app=dockercoins

k8s-scale:
	@echo "[+] Scaling worker deployment to $(or $(N),4) replicas..."
	kubectl scale deployment worker --replicas=$(or $(N),4)

k8s-clean:
	@echo "[+] Deleting DockerCoins microservices..."
	kubectl delete -f cluster-deployments/kubernetes-microservices/

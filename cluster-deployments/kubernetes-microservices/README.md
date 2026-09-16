# Kubernetes Microservices Orchestration (DockerCoins)

Declarative Kubernetes manifests for orchestrating the multi-tier **DockerCoins** distributed mining workload across Minikube, MicroK8s, or cloud-managed EKS/GKE clusters.

---

## Architecture Overview

```
                        [Web Browser]
                              |
                     (NodePort 30001)
                              v
                      [ webui Service ]
                              |
                              +----------------------------+
                                                           |
  +--------------------------------------------------------|-----------------+
  | Kubernetes Cluster (ClusterIP Internal Network)        |                 |
  |                                                        v                 |
  |  +--------------------+        +--------------------+  |                 |
  |  |    rng Service     |        |   hasher Service   |  |                 |
  |  |    (Port 80 TCP)   |        |    (Port 80 TCP)   |  |                 |
  |  +---------^----------+        +---------^----------+  |                 |
  |            |                             |             |                 |
  |            +--------------+--------------+             |                 |
  |                           |                            |                 |
  |                 +---------+---------+                  |                 |
  |                 |  worker (daemon)  |                  |                 |
  |                 |  (Replicas: 1-10) |                  |                 |
  |                 +---------+---------+                  |                 |
  |                           |                            |                 |
  |                           v (Port 6379)                |                 |
  |                   [ redis Service ] <------------------+                 |
  +--------------------------------------------------------------------------+
```

### Microservices Breakdown
- **`rng` (Python/Flask):** Generates random byte sequences (`GET /<n>`).
- **`hasher` (Ruby/Sinatra):** Computes SHA-256 hashes on submitted candidates (`POST /`).
- **`worker` (Python Daemon):** Continuous pipeline requesting bytes from `http://rng`, submitting them to `http://hasher`, and persisting winning mined blocks to `redis`.
- **`redis` (Alpine):** High-speed in-memory datastore tracking mined coin counters and block receipts.
- **`webui` (Node.js):** Dashboard rendering real-time mining hash-rate graphs.

---

## Deployment & Operational Runbook

### 1. Deploy All Services
```bash
kubectl apply -f rng-deployment.yaml
kubectl apply -f hasher-deployment.yaml
kubectl apply -f redis-deployment.yaml
kubectl apply -f worker-deployment.yaml
kubectl apply -f webui-deployment.yaml
```

### 2. Verify Pod & Service Status
```bash
kubectl get pods -l app=dockercoins -o wide
kubectl get svc -l app=dockercoins
```

### 3. Dynamic Replica Scaling
To evaluate CPU utilization and throughput scaling:
```bash
# Scale worker pods from 1 to 4 replicas
kubectl scale deployment worker --replicas=4

# Observe live pod creation and rescheduling
kubectl get pods -l role=worker -w
```

### 4. Self-Healing & Fault Tolerance Testing
Simulate node or container failure by forcefully terminating a worker pod:
```bash
POD_NAME=$(kubectl get pod -l role=worker -o jsonpath='{.items[0].metadata.name}')
kubectl delete pod $POD_NAME

# The Deployment ReplicaSet controller instantly recreates a replacement pod
kubectl get pods -l role=worker
```

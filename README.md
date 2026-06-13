# Dynamic-Batching Autoscaling Inference Server

A production-grade LLM inference server built with **Java 17 / Spring Boot 3** that collects individual requests into dynamic batches, maximising GPU utilisation and throughput. Deployed on **Kubernetes** with custom-metric **HPA autoscaling** driven by queue depth, validated by an async **load-generator** harness.

## Features

| Feature | Detail |
|---------|--------|
| Dynamic batching | `DynamicBatchScheduler` flushes when `maxBatchSize` is reached **or** `maxWaitMs` elapses, whichever comes first |
| Async API | `/v1/infer` returns a `CompletableFuture` — callers block only until their request completes within the batch |
| Prometheus metrics | `inference_queue_depth`, `inference_batch_size`, `inference_tokens_per_second`, `inference_request_latency_seconds` |
| Kubernetes HPA | Scales on the `inference_queue_depth` custom metric (via Prometheus Adapter) between 2–20 replicas |
| Load-generator harness | Python `aiohttp`-based async harness measures p50/p95/p99 latency and tokens/sec under configurable RPS |

## Architecture

```
Client ──► InferenceController
               │
               ▼
      DynamicBatchScheduler ◄── flush timer (maxWaitMs)
          │   queue             flush trigger (maxBatchSize)
          ▼
      BatchProcessor ──► [GPU inference backend / stub]
          │
          └──► CompletableFuture.complete() ──► HTTP response
```

## Quick Start

### Prerequisites
- Java 17+, Maven 3.8+
- Docker (for container builds)
- `kubectl` + a Kubernetes cluster (for K8s deployment)

### Run locally
```bash
mvn spring-boot:run
curl -X POST http://localhost:8080/v1/infer \
  -H "Content-Type: application/json" \
  -d '{"model":"llama3-8b","prompt":"Explain SOLID principles.","maxTokens":128}'
```

### Run the load generator
```bash
cd load-generator
pip install -r requirements.txt
python load_test.py --url http://localhost:8080 --rps 50 --duration 60
```

Expected output:
```
=== Load Test Results ===
Target RPS:     50
Duration:       60s
Total requests: 3000
Errors:         0 (0.0%)
p50 latency:    142.3 ms
p95 latency:    289.7 ms
p99 latency:    341.2 ms
Throughput:     4821.6 tokens/sec
```

## API

### `POST /v1/infer`
Submit an inference request. The request is automatically batched.

**Request:**
```json
{
  "model": "llama3-8b",
  "prompt": "Summarise the causes of World War I.",
  "maxTokens": 128,
  "temperature": 0.7
}
```

**Response:**
```json
{
  "requestId": "uuid",
  "generatedText": "...",
  "inputTokens": 9,
  "outputTokens": 128,
  "latencyMs": 180,
  "batchSize": 7
}
```

The `batchSize` field shows how many requests were packed into the same GPU call.

## Configuration

`application.yml`:

```yaml
inference:
  batch:
    max-batch-size: 8       # flush when this many requests are queued
    max-wait-ms: 50         # flush after this many ms even if batch is not full
    processing-threads: 2   # worker threads for batch execution
```

Environment variable overrides (Kubernetes):
```
INFERENCE_BATCH_MAX_BATCH_SIZE=16
INFERENCE_BATCH_MAX_WAIT_MS=25
```

## Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `inference_queue_depth` | Gauge | Requests currently waiting to be batched |
| `inference_batches_total` | Counter | Total number of batches dispatched |
| `inference_batch_size` | DistributionSummary | Distribution of batch sizes |
| `inference_request_latency_seconds` | Timer | End-to-end request latency (p50/p95/p99) |
| `inference_tokens_per_second` | Gauge | Rolling tokens/sec across all batches |

## Kubernetes Deployment

### Deploy the server
```bash
# Build and push your image
docker build -t your-registry/dynamic-batching-inference-server:latest .
docker push your-registry/dynamic-batching-inference-server:latest

# Deploy
kubectl apply -f k8s/deployment.yaml
kubectl apply -f k8s/service.yaml
```

### Enable custom-metric HPA
The HPA scales on the `inference_queue_depth` Prometheus metric via the
[Prometheus Adapter](https://github.com/kubernetes-sigs/prometheus-adapter).

```bash
kubectl apply -f k8s/custom-metrics-adapter.yaml
kubectl apply -f k8s/hpa.yaml

# Watch it scale
kubectl get hpa batching-inference-server-hpa --watch
```

### HPA behaviour
- Scales **up** by up to 4 pods/minute when average `inference_queue_depth > 10`
- Scales **down** conservatively: 2 pods every 2 minutes after a 5-minute stabilization window
- Min 2 / Max 20 replicas

## Project Structure

```
src/main/java/com/example/batchserver/
├── BatchServerApplication.java
├── config/
│   └── BatchProperties.java           # max-batch-size, max-wait-ms, threads
├── model/
│   ├── InferenceRequest.java
│   ├── BatchedRequest.java            # internal wrapper with CompletableFuture
│   └── InferenceResponse.java
├── batch/
│   ├── DynamicBatchScheduler.java     # queue + flush logic
│   └── BatchProcessor.java            # batch execution + metrics
└── controller/
    └── InferenceController.java
k8s/
├── deployment.yaml
├── service.yaml
├── hpa.yaml
└── custom-metrics-adapter.yaml
load-generator/
├── load_test.py
└── requirements.txt
```

## Development

```bash
mvn test
mvn package -DskipTests
docker build -t dynamic-batching-inference-server:latest .
```

> **Note:** `runInference()` in `BatchProcessor` is a simulation stub.
> Replace it with a real WebClient or gRPC call to your model backend.

## License

MIT

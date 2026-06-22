# Dynamic-Batching Autoscaling Inference Server

A production-grade LLM inference server built with **Java 17 / Spring Boot 3** that collects individual requests into dynamic batches, maximising GPU utilisation and throughput. Deployed on **Kubernetes** with custom-metric **HPA autoscaling** driven by queue depth, validated by an async **load-generator** harness.

## Features

| Feature | Detail |
|---------|--------|
| Dynamic batching | `DynamicBatchScheduler` flushes when `maxBatchSize` is reached **or** `maxWaitMs` elapses, whichever comes first |
| Admission control | Rejects excess work with HTTP `429` when the configured outstanding-request limit is reached |
| Request deadlines | Fails requests with HTTP `504` when their end-to-end inference deadline expires |
| Graceful shutdown | Stops admission, flushes queued work, and drains active requests before pod termination |
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
    max-outstanding-requests: 1024 # queued + pending + in-flight admission limit
    request-timeout-ms: 30000 # end-to-end deadline from admission to completion
    shutdown-grace-period-ms: 25000 # drain window before remaining requests fail
```

Environment variable overrides (Kubernetes):
```
INFERENCE_BATCH_MAX_BATCH_SIZE=16
INFERENCE_BATCH_MAX_WAIT_MS=25
INFERENCE_BATCH_MAX_OUTSTANDING_REQUESTS=1024
INFERENCE_BATCH_REQUEST_TIMEOUT_MS=30000
INFERENCE_BATCH_SHUTDOWN_GRACE_PERIOD_MS=25000
```

When capacity is exhausted, `/v1/infer` returns HTTP `429 Too Many Requests`,
a `Retry-After: 1` header, and:

```json
{
  "code": "CAPACITY_EXHAUSTED",
  "message": "Inference capacity is exhausted; maximum outstanding requests: 1024",
  "requestId": "uuid"
}
```

During shutdown, new requests receive HTTP `503 Service Unavailable`:

```json
{
  "code": "SERVICE_DRAINING",
  "message": "Inference server is draining and cannot accept new requests",
  "requestId": "uuid"
}
```

When the inference deadline expires, `/v1/infer` returns HTTP
`504 Gateway Timeout`:

```json
{
  "code": "REQUEST_TIMEOUT",
  "message": "Inference request exceeded its 30000ms deadline",
  "requestId": "uuid"
}
```

## Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `inference_queue_depth` | Gauge | Requests currently waiting to be batched |
| `inference_requests_outstanding` | Gauge | Accepted requests not yet completed |
| `inference_requests_rejected_total` | Counter | Requests rejected by reason |
| `inference_requests_timed_out_total` | Counter | Requests whose inference deadline expired |
| `inference_requests_failed_on_shutdown_total` | Counter | Requests failed after the drain grace period |
| `inference_scheduler_draining` | Gauge | `1` while the scheduler is draining, otherwise `0` |
| `inference_batches_total` | Counter | Total number of batches dispatched |
| `inference_batches_failed_total` | Counter | Batches that failed during processing |
| `inference_batches_in_flight` | Gauge | Batches currently executing |
| `inference_batch_size` | DistributionSummary | Distribution of batch sizes |
| `inference_batch_processing_seconds` | Timer | Backend batch processing duration |
| `inference_request_queue_wait_seconds` | Timer | Admission-to-worker wait, including executor backlog |
| `inference_request_latency_seconds` | Timer | End-to-end latency tagged by outcome |
| `inference_tokens_generated_total` | Counter | Successfully generated output tokens |
| `inference_tokens_per_second` | Gauge | Output throughput of the most recently completed batch |

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

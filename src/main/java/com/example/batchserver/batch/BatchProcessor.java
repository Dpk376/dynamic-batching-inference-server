package com.example.batchserver.batch;

import com.example.batchserver.model.BatchedRequest;
import com.example.batchserver.model.InferenceResponse;
import com.example.batchserver.model.TokenEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Component
public class BatchProcessor {

    private final MeterRegistry meterRegistry;
    private final Counter batchesTotal;
    private final Counter generatedTokensTotal;
    private final Timer batchProcessingTimer;
    private final Timer queueWaitTimer;
    private final AtomicInteger inFlightBatches = new AtomicInteger();
    private final AtomicReference<Double> latestTokensPerSecond = new AtomicReference<>(0.0);

    public BatchProcessor(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        this.batchesTotal = meterRegistry.counter("inference.batches.total");
        this.generatedTokensTotal = meterRegistry.counter("inference.tokens.generated");
        this.batchProcessingTimer = Timer.builder("inference.batch.processing")
                .description("Backend processing time per batch")
                .register(meterRegistry);
        this.queueWaitTimer = Timer.builder("inference.request.queue.wait")
                .description("Time from request admission until worker processing starts")
                .register(meterRegistry);
        Gauge.builder("inference.batches.in_flight", inFlightBatches, AtomicInteger::get)
                .description("Batches currently executing")
                .register(meterRegistry);
        Gauge.builder(
                        "inference.tokens_per_second",
                        latestTokensPerSecond,
                        AtomicReference::get)
                .description("Output token throughput of the most recently completed batch")
                .register(meterRegistry);
    }

    public void process(List<BatchedRequest> batch) {
        long processingStartedAtNanos = System.nanoTime();
        recordQueueWait(batch, processingStartedAtNanos);
        batchesTotal.increment();
        inFlightBatches.incrementAndGet();
        try {
            int generatedTokens = runInference(batch);
            meterRegistry.summary("inference.batch.size").record(batch.size());
            long processingNanos = System.nanoTime() - processingStartedAtNanos;
            generatedTokensTotal.increment(generatedTokens);
            updateTokensPerSecond(generatedTokens, processingNanos);
            log.debug("Batch of {} processed in {}ms",
                    batch.size(),
                    TimeUnit.NANOSECONDS.toMillis(processingNanos));
        } catch (Exception exception) {
            meterRegistry.counter("inference.batches.failed").increment();
            log.error("Batch processing failed: {}", exception.getMessage(), exception);
            for (BatchedRequest batchedRequest : batch) {
                batchedRequest.getEmitter().completeWithError(exception);
                batchedRequest.getLifecycleFuture().completeExceptionally(exception);
            }
        } finally {
            long processingNanos = System.nanoTime() - processingStartedAtNanos;
            batchProcessingTimer.record(processingNanos, TimeUnit.NANOSECONDS);
            inFlightBatches.decrementAndGet();
        }
    }

    private void recordQueueWait(List<BatchedRequest> batch, long processingStartedAtNanos) {
        for (BatchedRequest batchedRequest : batch) {
            long queueWaitNanos = processingStartedAtNanos - batchedRequest.getAdmittedAtNanos();
            queueWaitTimer.record(Math.max(queueWaitNanos, 0L), TimeUnit.NANOSECONDS);
        }
    }


    private void updateTokensPerSecond(int generatedTokens, long processingNanos) {
        if (processingNanos <= 0) {
            latestTokensPerSecond.set(0.0);
            return;
        }
        double processingSeconds = processingNanos / 1_000_000_000.0;
        latestTokensPerSecond.set(generatedTokens / processingSeconds);
    }

    // Simulation stub — replace with a real WebClient or gRPC call to your model backend
    private int runInference(List<BatchedRequest> batch) {
        long baseMs = 80 + (long) (Math.random() * 60);

        try {
            Thread.sleep(baseMs);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Batch inference was interrupted", exception);
        }

        int maxTokensInBatch = batch.stream()
                .mapToInt(req -> req.getRequest().getMaxTokens())
                .max().orElse(0);

        int generatedTokens = 0;

        for (int tokenIndex = 0; tokenIndex < maxTokensInBatch; tokenIndex++) {
            try {
                Thread.sleep(15);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Batch inference was interrupted", exception);
            }

            for (BatchedRequest batchedRequest : batch) {
                if (batchedRequest.getLifecycleFuture().isDone()) {
                    continue;
                }

                int requestMaxTokens = batchedRequest.getRequest().getMaxTokens();
                if (tokenIndex < requestMaxTokens) {
                    boolean isFinal = (tokenIndex == requestMaxTokens - 1);
                    TokenEvent event = new TokenEvent(
                            batchedRequest.getRequestId(),
                            " token" + tokenIndex,
                            tokenIndex,
                            isFinal
                    );

                    try {
                        batchedRequest.getEmitter().send(event);
                        generatedTokens++;
                        if (isFinal) {
                            batchedRequest.getEmitter().complete();
                            batchedRequest.getLifecycleFuture().complete(null);
                        }
                    } catch (Exception e) {
                        log.debug("Client disconnected for request {}", batchedRequest.getRequestId());
                        batchedRequest.getLifecycleFuture().completeExceptionally(e);
                    }
                }
            }
        }
        return generatedTokens;
    }

    private int countTokens(String text) {
        return text == null ? 0 : text.split("\\s+").length;
    }
}

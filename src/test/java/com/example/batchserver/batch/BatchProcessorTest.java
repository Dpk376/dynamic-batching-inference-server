package com.example.batchserver.batch;

import com.example.batchserver.model.BatchedRequest;
import com.example.batchserver.model.InferenceRequest;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class BatchProcessorTest {

    @Test
    void recordsBatchQueueTokenAndThroughputMetrics() throws Exception {
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        BatchProcessor batchProcessor = new BatchProcessor(meterRegistry);
        List<BatchedRequest> batch = buildBatch(2, 32);

        batchProcessor.process(batch);

        assertThat(meterRegistry.get("inference.batches.total").counter().count())
                .isEqualTo(1.0);
        assertThat(meterRegistry.get("inference.batch.size").summary().count())
                .isEqualTo(1L);
        assertThat(meterRegistry.get("inference.batch.size").summary().totalAmount())
                .isEqualTo(2.0);
        assertThat(meterRegistry.get("inference.request.queue.wait").timer().count())
                .isEqualTo(2L);
        assertThat(meterRegistry.get("inference.batch.processing").timer().count())
                .isEqualTo(1L);
        assertThat(meterRegistry.get("inference.tokens.generated").counter().count())
                .isEqualTo(64.0);
        assertThat(meterRegistry.get("inference.tokens_per_second").gauge().value())
                .isPositive();
        assertThat(meterRegistry.get("inference.batches.in_flight").gauge().value())
                .isZero();

        batch.get(0).getLifecycleFuture().get(1, TimeUnit.SECONDS);
    }

    @Test
    void updatesThroughputGaugeAfterEachCompletedBatch() {
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        BatchProcessor batchProcessor = new BatchProcessor(meterRegistry);

        batchProcessor.process(buildBatch(1, 1));
        double firstThroughput =
                meterRegistry.get("inference.tokens_per_second").gauge().value();

        batchProcessor.process(buildBatch(1, 1_000));
        double secondThroughput =
                meterRegistry.get("inference.tokens_per_second").gauge().value();

        assertThat(secondThroughput).isGreaterThan(firstThroughput);
        assertThat(meterRegistry.get("inference.tokens.generated").counter().count())
                .isEqualTo(1_001.0);
    }

    private List<BatchedRequest> buildBatch(int batchSize, int maxTokens) {
        List<BatchedRequest> batch = new ArrayList<>();
        for (int requestIndex = 0; requestIndex < batchSize; requestIndex++) {
            InferenceRequest request = new InferenceRequest();
            request.setRequestId(UUID.randomUUID().toString());
            request.setPrompt("test prompt");
            request.setMaxTokens(maxTokens);

            batch.add(new BatchedRequest(
                    request,
                    new SseEmitter(),
                    new CompletableFuture<>(),
                    System.nanoTime() - TimeUnit.MILLISECONDS.toNanos(20L)));
        }
        return batch;
    }
}

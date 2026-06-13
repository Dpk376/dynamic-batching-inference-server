package com.example.batchserver.batch;

import com.example.batchserver.model.BatchedRequest;
import com.example.batchserver.model.InferenceResponse;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class BatchProcessor {

    private static final Logger log = LoggerFactory.getLogger(BatchProcessor.class);

    private final MeterRegistry meterRegistry;

    public BatchProcessor(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    public void process(List<BatchedRequest> batch) {
        long start = System.currentTimeMillis();
        try {
            runInference(batch);
            long latencyMs = System.currentTimeMillis() - start;
            int totalTokens = batch.stream().mapToInt(r -> r.getRequest().getMaxTokens()).sum();

            meterRegistry.counter("inference.batches.total").increment();
            meterRegistry.summary("inference.batch.size").record(batch.size());
            if (latencyMs > 0) {
                meterRegistry.gauge("inference.tokens_per_second",
                        totalTokens / (latencyMs / 1000.0));
            }

            log.debug("Batch of {} processed in {}ms", batch.size(), latencyMs);
        } catch (Exception e) {
            log.error("Batch processing failed: {}", e.getMessage(), e);
            batch.forEach(r -> r.getFuture().completeExceptionally(e));
        }
    }

    // Simulation stub — replace with a real WebClient or gRPC call to your model backend
    private void runInference(List<BatchedRequest> batch) {
        long baseMs = 80 + (long) (Math.random() * 60);
        long perItemMs = 15;
        long totalMs = baseMs + perItemMs * batch.size();

        try {
            Thread.sleep(totalMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        for (BatchedRequest r : batch) {
            r.getFuture().complete(InferenceResponse.builder()
                    .requestId(r.getRequestId())
                    .generatedText("[ response for " + r.getRequestId() + " ]")
                    .inputTokens(countTokens(r.getRequest().getPrompt()))
                    .outputTokens(r.getRequest().getMaxTokens())
                    .latencyMs(totalMs)
                    .batchSize(batch.size())
                    .build());
        }
    }

    private int countTokens(String text) {
        return text == null ? 0 : text.split("\\s+").length;
    }
}

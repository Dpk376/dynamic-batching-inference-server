package com.example.batchserver.batch;

import com.example.batchserver.config.BatchProperties;
import com.example.batchserver.model.BatchedRequest;
import com.example.batchserver.model.InferenceRequest;
import com.example.batchserver.model.InferenceResponse;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doAnswer;

class DynamicBatchSchedulerTest {

    private BatchProcessor processor;
    private DynamicBatchScheduler scheduler;
    private BatchProperties props;

    @BeforeEach
    void setUp() {
        props = new BatchProperties();
        props.setMaxBatchSize(4);
        props.setMaxWaitMs(100L);
        props.setProcessingThreads(1);

        processor = Mockito.mock(BatchProcessor.class);
        doAnswer(inv -> {
            List<BatchedRequest> batch = inv.getArgument(0);
            batch.forEach(r -> r.getFuture().complete(
                    InferenceResponse.builder()
                            .requestId(r.getRequestId())
                            .generatedText("mock")
                            .outputTokens(10)
                            .batchSize(batch.size())
                            .build()));
            return null;
        }).when(processor).process(anyList());

        scheduler = new DynamicBatchScheduler(props, processor, new SimpleMeterRegistry());
        scheduler.start();
    }

    @AfterEach
    void tearDown() {
        scheduler.stop();
    }

    @Test
    void completesRequestWithinWaitWindow() throws Exception {
        InferenceRequest req = buildRequest();
        InferenceResponse response = scheduler.enqueue(req).get(2, TimeUnit.SECONDS);

        assertThat(response.getRequestId()).isEqualTo(req.getRequestId());
    }

    @Test
    void flushesImmediatelyWhenBatchFull() throws Exception {
        List<CompletableFuture<InferenceResponse>> futures = new ArrayList<>();
        for (int i = 0; i < props.getMaxBatchSize(); i++) {
            futures.add(scheduler.enqueue(buildRequest()));
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(2, TimeUnit.SECONDS);
        futures.forEach(f -> assertThat(f).isCompleted());
    }

    private InferenceRequest buildRequest() {
        InferenceRequest req = new InferenceRequest();
        req.setRequestId(UUID.randomUUID().toString());
        req.setPrompt("test prompt");
        req.setMaxTokens(64);
        return req;
    }
}

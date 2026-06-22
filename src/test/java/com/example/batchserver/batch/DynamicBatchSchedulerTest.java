package com.example.batchserver.batch;

import com.example.batchserver.config.BatchProperties;
import com.example.batchserver.model.BatchedRequest;
import com.example.batchserver.model.InferenceRequest;
import com.example.batchserver.model.TokenEvent;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doAnswer;

class DynamicBatchSchedulerTest {

    private BatchProcessor processor;
    private DynamicBatchScheduler scheduler;
    private BatchProperties props;
    private SimpleMeterRegistry meterRegistry;

    @BeforeEach
    void setUp() {
        props = new BatchProperties();
        props.setMaxBatchSize(4);
        props.setMaxWaitMs(100L);
        props.setProcessingThreads(1);
        props.setMaxOutstandingRequests(100);
        props.setRequestTimeoutMs(5_000L);
        props.setShutdownGracePeriodMs(1_000L);

        processor = Mockito.mock(BatchProcessor.class);
        doAnswer(inv -> {
            List<BatchedRequest> batch = inv.getArgument(0);
            batch.forEach(r -> {
                try {
                    r.getEmitter().send(new TokenEvent(r.getRequestId(), "mock", 0, true));
                    r.getEmitter().complete();
                    r.getLifecycleFuture().complete(null);
                } catch (Exception e) {}
            });
            return null;
        }).when(processor).process(anyList());

        meterRegistry = new SimpleMeterRegistry();
        scheduler = new DynamicBatchScheduler(props, processor, meterRegistry);
        scheduler.start();
    }

    @AfterEach
    void tearDown() {
        scheduler.stop();
    }

    private CompletableFuture<Void> getLifecycleFuture(String requestId) {
        return scheduler.getActiveRequests().stream()
                .filter(r -> r.getRequestId().equals(requestId))
                .map(BatchedRequest::getLifecycleFuture)
                .findFirst()
                .orElse(null);
    }

    @Test
    void completesRequestWithinWaitWindow() throws Exception {
        InferenceRequest req = buildRequest();
        scheduler.enqueue(req);
        
        CompletableFuture<Void> future = getLifecycleFuture(req.getRequestId());
        assertThat(future).isNotNull();
        future.get(2, TimeUnit.SECONDS);

        assertThat(meterRegistry.get("inference.request.latency")
                .tag("outcome", "success")
                .timer()
                .count()).isEqualTo(1L);
    }

    @Test
    void flushesImmediatelyWhenBatchFull() throws Exception {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < props.getMaxBatchSize(); i++) {
            InferenceRequest req = buildRequest();
            scheduler.enqueue(req);
            futures.add(getLifecycleFuture(req.getRequestId()));
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(2, TimeUnit.SECONDS);
        futures.forEach(f -> assertThat(f).isCompleted());
    }

    @Test
    void rejectsRequestWhenOutstandingCapacityIsExhausted() {
        SimpleMeterRegistry capacityMeterRegistry = new SimpleMeterRegistry();
        DynamicBatchScheduler capacityLimitedScheduler =
                buildCapacityLimitedScheduler(2, capacityMeterRegistry);
        try {
            capacityLimitedScheduler.enqueue(buildRequest());
            capacityLimitedScheduler.enqueue(buildRequest());

            assertThatThrownBy(() -> capacityLimitedScheduler.enqueue(buildRequest()))
                    .isInstanceOf(SchedulerOverloadedException.class)
                    .hasMessageContaining("maximum outstanding requests: 2");
            assertThat(capacityMeterRegistry.get("inference.requests.outstanding")
                    .gauge()
                    .value()).isEqualTo(2.0);
            assertThat(capacityMeterRegistry.get("inference.requests.rejected")
                    .tag("reason", "capacity")
                    .counter()
                    .count()).isEqualTo(1.0);
        } finally {
            capacityLimitedScheduler.stop();
        }
    }

    @Test
    void releasesCapacityWhenRequestIsCancelled() {
        SimpleMeterRegistry capacityMeterRegistry = new SimpleMeterRegistry();
        DynamicBatchScheduler capacityLimitedScheduler =
                buildCapacityLimitedScheduler(1, capacityMeterRegistry);
        try {
            InferenceRequest req1 = buildRequest();
            SseEmitter firstEmitter = capacityLimitedScheduler.enqueue(req1);
            
            CompletableFuture<Void> firstFuture = capacityLimitedScheduler.getActiveRequests().stream()
                    .filter(r -> r.getRequestId().equals(req1.getRequestId()))
                    .map(BatchedRequest::getLifecycleFuture)
                    .findFirst().orElseThrow();

            assertThatThrownBy(() -> capacityLimitedScheduler.enqueue(buildRequest()))
                    .isInstanceOf(SchedulerOverloadedException.class);

            firstFuture.cancel(true);

            SseEmitter acceptedAfterCancellation =
                    capacityLimitedScheduler.enqueue(buildRequest());
            assertThat(acceptedAfterCancellation).isNotNull();
            assertThat(capacityMeterRegistry.get("inference.requests.outstanding")
                    .gauge()
                    .value()).isEqualTo(1.0);
        } finally {
            capacityLimitedScheduler.stop();
        }
    }

    @Test
    void timesOutQueuedRequestAndReleasesCapacity() throws Exception {
        SimpleMeterRegistry timeoutMeterRegistry = new SimpleMeterRegistry();
        DynamicBatchScheduler timeoutScheduler = buildTimeoutScheduler(
                1,
                10_000L,
                30L,
                Mockito.mock(BatchProcessor.class),
                timeoutMeterRegistry);
        try {
            InferenceRequest req = buildRequest();
            timeoutScheduler.enqueue(req);
            CompletableFuture<Void> timedOutFuture = timeoutScheduler.getActiveRequests().stream()
                    .filter(r -> r.getRequestId().equals(req.getRequestId()))
                    .map(BatchedRequest::getLifecycleFuture)
                    .findFirst().orElseThrow();

            assertThatThrownBy(() -> timedOutFuture.get(2, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(InferenceRequestTimeoutException.class);
            assertThat(timeoutMeterRegistry.get("inference.requests.outstanding")
                    .gauge()
                    .value()).isZero();
            assertThat(timeoutMeterRegistry.get("inference.queue.depth")
                    .gauge()
                    .value()).isZero();
            assertThat(timeoutMeterRegistry.get("inference.requests.timed_out")
                    .counter()
                    .count()).isEqualTo(1.0);
            assertThat(timeoutMeterRegistry.get("inference.request.latency")
                    .tag("outcome", "timeout")
                    .timer()
                    .count()).isEqualTo(1L);

            SseEmitter acceptedAfterTimeout =
                    timeoutScheduler.enqueue(buildRequest());
            assertThat(acceptedAfterTimeout).isNotNull();
        } finally {
            timeoutScheduler.stop();
        }
    }

    @Test
    void ignoresBackendCompletionAfterRequestTimeout() throws Exception {
        CountDownLatch processingStarted = new CountDownLatch(1);
        CountDownLatch releaseProcessor = new CountDownLatch(1);
        BatchProcessor delayedProcessor = Mockito.mock(BatchProcessor.class);
        doAnswer(invocation -> {
            List<BatchedRequest> batch = invocation.getArgument(0);
            processingStarted.countDown();
            releaseProcessor.await(2, TimeUnit.SECONDS);
            for (BatchedRequest batchedRequest : batch) {
                try {
                    batchedRequest.getEmitter().send(new TokenEvent(batchedRequest.getRequestId(), "late response", 0, true));
                    batchedRequest.getEmitter().complete();
                    batchedRequest.getLifecycleFuture().complete(null);
                } catch (Exception e) {}
            }
            return null;
        }).when(delayedProcessor).process(anyList());

        SimpleMeterRegistry timeoutMeterRegistry = new SimpleMeterRegistry();
        DynamicBatchScheduler timeoutScheduler = buildTimeoutScheduler(
                1,
                10L,
                100L,
                delayedProcessor,
                timeoutMeterRegistry);
        try {
            InferenceRequest req = buildRequest();
            timeoutScheduler.enqueue(req);
            CompletableFuture<Void> timedOutFuture = timeoutScheduler.getActiveRequests().stream()
                    .filter(r -> r.getRequestId().equals(req.getRequestId()))
                    .map(BatchedRequest::getLifecycleFuture)
                    .findFirst().orElseThrow();
                    
            assertThat(processingStarted.await(1, TimeUnit.SECONDS)).isTrue();

            assertThatThrownBy(() -> timedOutFuture.get(2, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(InferenceRequestTimeoutException.class);
            releaseProcessor.countDown();

            assertThat(timeoutMeterRegistry.get("inference.requests.outstanding")
                    .gauge()
                    .value()).isZero();
            assertThat(timedOutFuture).isCompletedExceptionally();
        } finally {
            releaseProcessor.countDown();
            timeoutScheduler.stop();
        }
    }

    @Test
    void flushesQueuedRequestsDuringGracefulShutdown() throws Exception {
        SimpleMeterRegistry shutdownMeterRegistry = new SimpleMeterRegistry();
        BatchProcessor completingProcessor = Mockito.mock(BatchProcessor.class);
        doAnswer(invocation -> {
            List<BatchedRequest> batch = invocation.getArgument(0);
            for (BatchedRequest batchedRequest : batch) {
                try {
                    batchedRequest.getEmitter().send(new TokenEvent(batchedRequest.getRequestId(), "completed during drain", 0, true));
                    batchedRequest.getEmitter().complete();
                    batchedRequest.getLifecycleFuture().complete(null);
                } catch (Exception e) {}
            }
            return null;
        }).when(completingProcessor).process(anyList());

        DynamicBatchScheduler drainingScheduler = buildScheduler(
                1,
                10_000L,
                30_000L,
                1_000L,
                completingProcessor,
                shutdownMeterRegistry);
        InferenceRequest req = buildRequest();
        drainingScheduler.enqueue(req);
        CompletableFuture<Void> queuedFuture = drainingScheduler.getActiveRequests().stream()
                .filter(r -> r.getRequestId().equals(req.getRequestId()))
                .map(BatchedRequest::getLifecycleFuture)
                .findFirst().orElseThrow();

        drainingScheduler.stop();

        queuedFuture.get(1, TimeUnit.SECONDS);
        assertThat(shutdownMeterRegistry.get("inference.scheduler.draining")
                .gauge()
                .value()).isZero();
    }

    @Test
    void failsRemainingRequestsAfterShutdownGracePeriod() {
        SimpleMeterRegistry shutdownMeterRegistry = new SimpleMeterRegistry();
        BatchProcessor blockedProcessor = Mockito.mock(BatchProcessor.class);
        doAnswer(invocation -> {
            Thread.sleep(5_000L);
            return null;
        }).when(blockedProcessor).process(anyList());

        DynamicBatchScheduler drainingScheduler = buildScheduler(
                1,
                1L,
                30_000L,
                30L,
                blockedProcessor,
                shutdownMeterRegistry);
        InferenceRequest request = buildRequest();
        drainingScheduler.enqueue(request);
        CompletableFuture<Void> pendingFuture = drainingScheduler.getActiveRequests().stream()
                .filter(r -> r.getRequestId().equals(request.getRequestId()))
                .map(BatchedRequest::getLifecycleFuture)
                .findFirst().orElseThrow();

        drainingScheduler.stop();

        assertThatThrownBy(pendingFuture::join)
                .hasCauseInstanceOf(SchedulerDrainingException.class);
        assertThatThrownBy(() -> drainingScheduler.enqueue(buildRequest()))
                .isInstanceOf(SchedulerDrainingException.class);
        assertThat(shutdownMeterRegistry.get("inference.requests.failed_on_shutdown")
                .counter()
                .count()).isEqualTo(1.0);
        assertThat(shutdownMeterRegistry.get("inference.requests.rejected")
                .tag("reason", "draining")
                .counter()
                .count()).isEqualTo(1.0);
    }

    private DynamicBatchScheduler buildCapacityLimitedScheduler(
            int maxOutstandingRequests,
            SimpleMeterRegistry capacityMeterRegistry) {
        BatchProperties limitedProperties = new BatchProperties();
        limitedProperties.setMaxBatchSize(8);
        limitedProperties.setMaxWaitMs(10_000L);
        limitedProperties.setProcessingThreads(1);
        limitedProperties.setMaxOutstandingRequests(maxOutstandingRequests);
        limitedProperties.setRequestTimeoutMs(30_000L);

        BatchProcessor nonCompletingProcessor = Mockito.mock(BatchProcessor.class);
        return buildTimeoutScheduler(
                maxOutstandingRequests,
                limitedProperties.getMaxWaitMs(),
                limitedProperties.getRequestTimeoutMs(),
                nonCompletingProcessor,
                capacityMeterRegistry);
    }

    private DynamicBatchScheduler buildTimeoutScheduler(
            int maxOutstandingRequests,
            long maxWaitMs,
            long requestTimeoutMs,
            BatchProcessor batchProcessor,
            SimpleMeterRegistry schedulerMeterRegistry) {
        return buildScheduler(
                maxOutstandingRequests,
                maxWaitMs,
                requestTimeoutMs,
                1_000L,
                batchProcessor,
                schedulerMeterRegistry);
    }

    private DynamicBatchScheduler buildScheduler(
            int maxOutstandingRequests,
            long maxWaitMs,
            long requestTimeoutMs,
            long shutdownGracePeriodMs,
            BatchProcessor batchProcessor,
            SimpleMeterRegistry schedulerMeterRegistry) {
        BatchProperties schedulerProperties = new BatchProperties();
        schedulerProperties.setMaxBatchSize(8);
        schedulerProperties.setMaxWaitMs(maxWaitMs);
        schedulerProperties.setProcessingThreads(1);
        schedulerProperties.setMaxOutstandingRequests(maxOutstandingRequests);
        schedulerProperties.setRequestTimeoutMs(requestTimeoutMs);
        schedulerProperties.setShutdownGracePeriodMs(shutdownGracePeriodMs);

        DynamicBatchScheduler timeoutScheduler = new DynamicBatchScheduler(
                schedulerProperties,
                batchProcessor,
                schedulerMeterRegistry);
        timeoutScheduler.start();
        return timeoutScheduler;
    }

    private InferenceRequest buildRequest() {
        InferenceRequest req = new InferenceRequest();
        req.setRequestId(UUID.randomUUID().toString());
        req.setPrompt("test prompt");
        req.setMaxTokens(64);
        return req;
    }
}

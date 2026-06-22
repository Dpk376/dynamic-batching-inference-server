package com.example.batchserver.batch;

import com.example.batchserver.config.BatchProperties;
import com.example.batchserver.model.BatchedRequest;
import com.example.batchserver.model.InferenceRequest;
import com.example.batchserver.model.InferenceResponse;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Component
public class DynamicBatchScheduler {

    private final BatchProperties props;
    private final BatchProcessor processor;
    private final MeterRegistry meterRegistry;

    private final Object lifecycleLock = new Object();
    private final Object completionMonitor = new Object();
    private final LinkedBlockingQueue<BatchedRequest> queue = new LinkedBlockingQueue<>();
    private final Set<BatchedRequest> activeRequests = ConcurrentHashMap.newKeySet();
    private final AtomicInteger outstandingRequests = new AtomicInteger();
    private final AtomicInteger draining = new AtomicInteger();
    private final ScheduledThreadPoolExecutor controlExecutor = new ScheduledThreadPoolExecutor(
            1,
            runnable -> new Thread(runnable, "batch-control"));
    private boolean acceptingRequests;
    private ScheduledFuture<?> periodicFlushTask;
    private Semaphore admissionPermits;
    private ExecutorService workers;

    public DynamicBatchScheduler(BatchProperties props, BatchProcessor processor, MeterRegistry meterRegistry) {
        this.props = props;
        this.processor = processor;
        this.meterRegistry = meterRegistry;
    }

    Set<BatchedRequest> getActiveRequests() {
        return activeRequests;
    }

    @PostConstruct
    public void start() {
        admissionPermits = new Semaphore(props.getMaxOutstandingRequests(), true);
        workers = Executors.newFixedThreadPool(props.getProcessingThreads());
        controlExecutor.setRemoveOnCancelPolicy(true);
        acceptingRequests = true;

        Gauge.builder("inference.queue.depth", queue, LinkedBlockingQueue::size)
                .description("Requests waiting to be batched")
                .register(meterRegistry);
        Gauge.builder("inference.requests.outstanding", outstandingRequests, AtomicInteger::get)
                .description("Accepted requests that have not completed")
                .register(meterRegistry);
        Gauge.builder("inference.scheduler.draining", draining, AtomicInteger::get)
                .description("Whether the scheduler is draining for shutdown")
                .register(meterRegistry);

        periodicFlushTask = controlExecutor.scheduleAtFixedRate(
                this::flushBatch,
                props.getMaxWaitMs(),
                props.getMaxWaitMs(),
                TimeUnit.MILLISECONDS);

        log.info(
                "Batch scheduler started: maxBatchSize={}, maxWaitMs={}, "
                        + "maxOutstandingRequests={}, requestTimeoutMs={}, shutdownGracePeriodMs={}",
                props.getMaxBatchSize(),
                props.getMaxWaitMs(),
                props.getMaxOutstandingRequests(),
                props.getRequestTimeoutMs(),
                props.getShutdownGracePeriodMs());
    }

    public SseEmitter enqueue(InferenceRequest request) {
        synchronized (lifecycleLock) {
            ensureAcceptingRequests(request);
            return admitRequest(request);
        }
    }

    private void ensureAcceptingRequests(InferenceRequest request) {
        if (!acceptingRequests) {
            meterRegistry.counter("inference.requests.rejected", "reason", "draining").increment();
            throw new SchedulerDrainingException(request.getRequestId());
        }
    }

    private SseEmitter admitRequest(InferenceRequest request) {
        if (!admissionPermits.tryAcquire()) {
            meterRegistry.counter("inference.requests.rejected", "reason", "capacity").increment();
            throw new SchedulerOverloadedException(request.getRequestId(), props.getMaxOutstandingRequests());
        }
        
        SseEmitter emitter = new SseEmitter(props.getRequestTimeoutMs());
        CompletableFuture<Void> lifecycleFuture = new CompletableFuture<>();
        outstandingRequests.incrementAndGet();

        BatchedRequest batchedRequest = new BatchedRequest(request, emitter, lifecycleFuture, System.nanoTime());
        activeRequests.add(batchedRequest);
        
        emitter.onCompletion(() -> lifecycleFuture.complete(null));
        emitter.onTimeout(() -> lifecycleFuture.completeExceptionally(new InferenceRequestTimeoutException(request.getRequestId(), props.getRequestTimeoutMs())));
        emitter.onError(lifecycleFuture::completeExceptionally);
        
        lifecycleFuture.whenComplete((response, error) -> {
            recordRequestLatency(batchedRequest, error);
            releaseRequest(batchedRequest);
        });
        queue.offer(batchedRequest);
        ScheduledFuture<?> timeoutTask = controlExecutor.schedule(
                () -> timeoutRequest(batchedRequest),
                props.getRequestTimeoutMs(),
                TimeUnit.MILLISECONDS);
        lifecycleFuture.whenComplete((response, error) -> timeoutTask.cancel(false));

        if (queue.size() >= props.getMaxBatchSize()) {
            controlExecutor.submit(this::flushBatch);
        }
        return emitter;
    }

    private void recordRequestLatency(BatchedRequest batchedRequest, Throwable error) {
        long latencyNanos = System.nanoTime() - batchedRequest.getAdmittedAtNanos();
        Timer.builder("inference.request.latency")
                .description("End-to-end inference request latency")
                .tag("outcome", classifyOutcome(error))
                .register(meterRegistry)
                .record(latencyNanos, TimeUnit.NANOSECONDS);
    }

    private String classifyOutcome(Throwable error) {
        if (error == null) {
            return "success";
        }
        if (error instanceof CancellationException) {
            return "cancelled";
        }
        if (error instanceof InferenceRequestTimeoutException) {
            return "timeout";
        }
        if (error instanceof SchedulerDrainingException) {
            return "shutdown";
        }
        return "failed";
    }

    private void releaseRequest(BatchedRequest batchedRequest) {
        activeRequests.remove(batchedRequest);
        outstandingRequests.decrementAndGet();
        admissionPermits.release();
        synchronized (completionMonitor) {
            completionMonitor.notifyAll();
        }
    }

    private void timeoutRequest(BatchedRequest batchedRequest) {
        InferenceRequestTimeoutException timeoutException = new InferenceRequestTimeoutException(
                batchedRequest.getRequestId(),
                props.getRequestTimeoutMs());
        
        boolean timedOut = batchedRequest.getLifecycleFuture().completeExceptionally(timeoutException);
        if (!timedOut) {
            return;
        }

        batchedRequest.getEmitter().completeWithError(timeoutException);
        queue.remove(batchedRequest);
        meterRegistry.counter("inference.requests.timed_out").increment();
        log.debug("Inference request timed out: requestId={}", batchedRequest.getRequestId());
    }

    private synchronized void flushBatch() {
        dispatchBatch();
    }

    private void dispatchBatch() {
        if (queue.isEmpty() || workers.isShutdown()) return;
        List<BatchedRequest> batch = new ArrayList<>(props.getMaxBatchSize());
        queue.drainTo(batch, props.getMaxBatchSize());
        if (batch.isEmpty()) return;
        log.debug("Dispatching batch of {} requests", batch.size());
        workers.submit(() -> processActiveRequests(batch));
    }

    private void processActiveRequests(List<BatchedRequest> batch) {
        List<BatchedRequest> activeRequests = new ArrayList<>(batch.size());
        for (BatchedRequest batchedRequest : batch) {
            if (!batchedRequest.getLifecycleFuture().isDone()) {
                activeRequests.add(batchedRequest);
            }
        }
        if (!activeRequests.isEmpty()) {
            processor.process(activeRequests);
        }
    }

    @PreDestroy
    public void stop() {
        synchronized (lifecycleLock) {
            if (!acceptingRequests) {
                return;
            }
            acceptingRequests = false;
            draining.set(1);
            if (periodicFlushTask != null) {
                periodicFlushTask.cancel(false);
            }
            flushAllQueuedRequests();
            workers.shutdown();
        }

        log.info("Inference scheduler draining: outstandingRequests={}, gracePeriodMs={}",
                outstandingRequests.get(), props.getShutdownGracePeriodMs());
        waitForOutstandingRequests();
        failRemainingRequests();
        controlExecutor.shutdownNow();
        workers.shutdownNow();
        draining.set(0);
        log.info("Inference scheduler stopped: outstandingRequests={}", outstandingRequests.get());
    }

    private synchronized void flushAllQueuedRequests() {
        while (!queue.isEmpty()) {
            dispatchBatch();
        }
    }

    private void waitForOutstandingRequests() {
        long deadlineNanos = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(props.getShutdownGracePeriodMs());
        synchronized (completionMonitor) {
            while (outstandingRequests.get() > 0) {
                long remainingNanos = deadlineNanos - System.nanoTime();
                if (remainingNanos <= 0) {
                    return;
                }
                try {
                    TimeUnit.NANOSECONDS.timedWait(completionMonitor, remainingNanos);
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    private void failRemainingRequests() {
        int failedRequests = 0;
        for (BatchedRequest batchedRequest : activeRequests) {
            SchedulerDrainingException shutdownException =
                    new SchedulerDrainingException(batchedRequest.getRequestId());
            if (batchedRequest.getLifecycleFuture().completeExceptionally(shutdownException)) {
                batchedRequest.getEmitter().completeWithError(shutdownException);
                failedRequests++;
            }
        }
        queue.clear();
        if (failedRequests > 0) {
            meterRegistry.counter("inference.requests.failed_on_shutdown")
                    .increment(failedRequests);
            log.warn("Forced {} inference requests to fail after shutdown grace period",
                    failedRequests);
        }
    }
}

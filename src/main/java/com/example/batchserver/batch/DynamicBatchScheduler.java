package com.example.batchserver.batch;

import com.example.batchserver.config.BatchProperties;
import com.example.batchserver.model.BatchedRequest;
import com.example.batchserver.model.InferenceRequest;
import com.example.batchserver.model.InferenceResponse;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

@Component
public class DynamicBatchScheduler {

    private static final Logger log = LoggerFactory.getLogger(DynamicBatchScheduler.class);

    private final BatchProperties props;
    private final BatchProcessor processor;
    private final MeterRegistry meterRegistry;

    private final LinkedBlockingQueue<BatchedRequest> queue = new LinkedBlockingQueue<>();
    private final ScheduledExecutorService flusher = Executors.newSingleThreadScheduledExecutor(
            r -> new Thread(r, "batch-flusher"));
    private ExecutorService workers;

    public DynamicBatchScheduler(BatchProperties props, BatchProcessor processor, MeterRegistry meterRegistry) {
        this.props = props;
        this.processor = processor;
        this.meterRegistry = meterRegistry;
    }

    @PostConstruct
    public void start() {
        workers = Executors.newFixedThreadPool(props.getProcessingThreads());

        Gauge.builder("inference.queue.depth", queue, LinkedBlockingQueue::size)
                .description("Requests waiting to be batched")
                .register(meterRegistry);

        flusher.scheduleAtFixedRate(
                this::flushBatch,
                props.getMaxWaitMs(),
                props.getMaxWaitMs(),
                TimeUnit.MILLISECONDS);

        log.info("Batch scheduler started: maxBatchSize={}, maxWaitMs={}",
                props.getMaxBatchSize(), props.getMaxWaitMs());
    }

    public CompletableFuture<InferenceResponse> enqueue(InferenceRequest request) {
        CompletableFuture<InferenceResponse> future = new CompletableFuture<>();
        queue.offer(new BatchedRequest(request, future));
        if (queue.size() >= props.getMaxBatchSize()) {
            flusher.submit(this::flushBatch);
        }
        return future;
    }

    private synchronized void flushBatch() {
        if (queue.isEmpty()) return;
        List<BatchedRequest> batch = new ArrayList<>(props.getMaxBatchSize());
        queue.drainTo(batch, props.getMaxBatchSize());
        if (batch.isEmpty()) return;
        log.debug("Dispatching batch of {} requests", batch.size());
        workers.submit(() -> processor.process(batch));
    }

    @PreDestroy
    public void stop() {
        flusher.shutdownNow();
        if (workers != null) workers.shutdownNow();
    }
}

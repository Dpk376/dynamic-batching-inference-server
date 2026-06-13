package com.example.batchserver.controller;

import com.example.batchserver.batch.DynamicBatchScheduler;
import com.example.batchserver.model.InferenceRequest;
import com.example.batchserver.model.InferenceResponse;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/v1")
public class InferenceController {

    private final DynamicBatchScheduler scheduler;

    public InferenceController(DynamicBatchScheduler scheduler) {
        this.scheduler = scheduler;
    }

    @PostMapping("/infer")
    public CompletableFuture<ResponseEntity<InferenceResponse>> infer(@RequestBody InferenceRequest request) {
        if (request.getRequestId() == null) {
            request.setRequestId(UUID.randomUUID().toString());
        }
        return scheduler.enqueue(request).thenApply(ResponseEntity::ok);
    }
}

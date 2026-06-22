package com.example.batchserver.model;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import java.util.concurrent.CompletableFuture;

@Getter
@RequiredArgsConstructor
public class BatchedRequest {
    private final InferenceRequest request;
    private final SseEmitter emitter;
    private final CompletableFuture<Void> lifecycleFuture;
    private final long admittedAtNanos;

    public String getRequestId() {
        return request.getRequestId();
    }
}

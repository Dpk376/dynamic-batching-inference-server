package com.example.batchserver.model;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.concurrent.CompletableFuture;

@Getter
@RequiredArgsConstructor
public class BatchedRequest {
    private final InferenceRequest request;
    private final CompletableFuture<InferenceResponse> future;

    public String getRequestId() {
        return request.getRequestId();
    }
}

package com.example.batchserver.batch;

public class InferenceRequestTimeoutException extends RuntimeException {

    private final String requestId;

    public InferenceRequestTimeoutException(String requestId, long requestTimeoutMs) {
        super("Inference request exceeded its " + requestTimeoutMs + "ms deadline");
        this.requestId = requestId;
    }

    public String getRequestId() {
        return requestId;
    }
}

package com.example.batchserver.batch;

public class SchedulerDrainingException extends RuntimeException {

    private final String requestId;

    public SchedulerDrainingException(String requestId) {
        super("Inference server is draining and cannot accept new requests");
        this.requestId = requestId;
    }

    public String getRequestId() {
        return requestId;
    }
}

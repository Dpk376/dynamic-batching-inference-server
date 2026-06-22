package com.example.batchserver.batch;

public class SchedulerOverloadedException extends RuntimeException {

    private final String requestId;

    public SchedulerOverloadedException(String requestId, int maxOutstandingRequests) {
        super("Inference capacity is exhausted; maximum outstanding requests: "
                + maxOutstandingRequests);
        this.requestId = requestId;
    }

    public String getRequestId() {
        return requestId;
    }
}

package com.example.batchserver.controller;

import com.example.batchserver.batch.InferenceRequestTimeoutException;
import com.example.batchserver.batch.SchedulerDrainingException;
import com.example.batchserver.batch.SchedulerOverloadedException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class InferenceExceptionHandler {

    private static final String CAPACITY_EXHAUSTED_CODE = "CAPACITY_EXHAUSTED";
    private static final String REQUEST_TIMEOUT_CODE = "REQUEST_TIMEOUT";
    private static final String SERVICE_DRAINING_CODE = "SERVICE_DRAINING";
    private static final String RETRY_AFTER_SECONDS = "1";

    @ExceptionHandler(SchedulerOverloadedException.class)
    public ResponseEntity<ApiErrorResponse> handleSchedulerOverloaded(
            SchedulerOverloadedException exception) {
        ApiErrorResponse errorResponse = new ApiErrorResponse(
                CAPACITY_EXHAUSTED_CODE,
                exception.getMessage(),
                exception.getRequestId());

        return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                .header(HttpHeaders.RETRY_AFTER, RETRY_AFTER_SECONDS)
                .body(errorResponse);
    }

    @ExceptionHandler(InferenceRequestTimeoutException.class)
    public ResponseEntity<ApiErrorResponse> handleInferenceRequestTimeout(
            InferenceRequestTimeoutException exception) {
        ApiErrorResponse errorResponse = new ApiErrorResponse(
                REQUEST_TIMEOUT_CODE,
                exception.getMessage(),
                exception.getRequestId());

        return ResponseEntity.status(HttpStatus.GATEWAY_TIMEOUT)
                .body(errorResponse);
    }

    @ExceptionHandler(SchedulerDrainingException.class)
    public ResponseEntity<ApiErrorResponse> handleSchedulerDraining(
            SchedulerDrainingException exception) {
        ApiErrorResponse errorResponse = new ApiErrorResponse(
                SERVICE_DRAINING_CODE,
                exception.getMessage(),
                exception.getRequestId());

        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .header(HttpHeaders.RETRY_AFTER, RETRY_AFTER_SECONDS)
                .body(errorResponse);
    }
}

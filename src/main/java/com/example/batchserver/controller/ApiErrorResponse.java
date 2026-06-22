package com.example.batchserver.controller;

public record ApiErrorResponse(
        String code,
        String message,
        String requestId
) {
}

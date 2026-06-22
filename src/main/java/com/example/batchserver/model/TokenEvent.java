package com.example.batchserver.model;

public record TokenEvent(
    String requestId,
    String token,
    int tokenIndex,
    boolean isFinal
) {}

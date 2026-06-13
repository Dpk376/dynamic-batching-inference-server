package com.example.batchserver.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class InferenceResponse {
    private String requestId;
    private String generatedText;
    private int inputTokens;
    private int outputTokens;
    private long latencyMs;
    private int batchSize;
}

package com.example.batchserver.model;

import lombok.Data;

@Data
public class InferenceRequest {
    private String requestId;
    private String model;
    private String prompt;
    private int maxTokens = 256;
    private double temperature = 0.7;
}

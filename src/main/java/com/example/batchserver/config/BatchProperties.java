package com.example.batchserver.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "inference.batch")
public class BatchProperties {
    private int maxBatchSize = 8;
    private long maxWaitMs = 50L;
    private int processingThreads = 2;
}

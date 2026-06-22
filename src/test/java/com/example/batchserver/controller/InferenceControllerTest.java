package com.example.batchserver.controller;

import com.example.batchserver.batch.DynamicBatchScheduler;
import com.example.batchserver.batch.InferenceRequestTimeoutException;
import com.example.batchserver.batch.SchedulerDrainingException;
import com.example.batchserver.batch.SchedulerOverloadedException;
import com.example.batchserver.model.InferenceResponse;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(InferenceController.class)
@Import(InferenceExceptionHandler.class)
class InferenceControllerTest {

    private static final String REQUEST_ID = "request-123";

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private DynamicBatchScheduler scheduler;

    @Test
    void returnsTooManyRequestsWhenSchedulerCapacityIsExhausted() throws Exception {
        when(scheduler.enqueue(any()))
                .thenThrow(new SchedulerOverloadedException(REQUEST_ID, 100));

        mockMvc.perform(post("/v1/infer")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "requestId": "request-123",
                                  "model": "llama3-8b",
                                  "prompt": "Explain dynamic batching",
                                  "maxTokens": 64
                                }
                                """))
                .andExpect(status().isTooManyRequests())
                .andExpect(header().string(HttpHeaders.RETRY_AFTER, "1"))
                .andExpect(jsonPath("$.code").value("CAPACITY_EXHAUSTED"))
                .andExpect(jsonPath("$.requestId").value(REQUEST_ID))
                .andExpect(jsonPath("$.message")
                        .value("Inference capacity is exhausted; maximum outstanding requests: 100"));
    }

    @Test
    void returnsGatewayTimeoutWhenInferenceDeadlineExpires() throws Exception {
        SseEmitter emitter = new SseEmitter();
        emitter.completeWithError(new InferenceRequestTimeoutException(REQUEST_ID, 30_000L));
        when(scheduler.enqueue(any())).thenReturn(emitter);

        MvcResult asyncResult = mockMvc.perform(post("/v1/infer")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "requestId": "request-123",
                                  "model": "llama3-8b",
                                  "prompt": "Explain dynamic batching",
                                  "maxTokens": 64
                                }
                                """))
                .andExpect(request().asyncStarted())
                .andReturn();

        mockMvc.perform(asyncDispatch(asyncResult))
                .andExpect(status().isGatewayTimeout())
                .andExpect(jsonPath("$.code").value("REQUEST_TIMEOUT"))
                .andExpect(jsonPath("$.requestId").value(REQUEST_ID))
                .andExpect(jsonPath("$.message")
                        .value("Inference request exceeded its 30000ms deadline"));
    }

    @Test
    void returnsServiceUnavailableWhenSchedulerIsDraining() throws Exception {
        when(scheduler.enqueue(any()))
                .thenThrow(new SchedulerDrainingException(REQUEST_ID));

        mockMvc.perform(post("/v1/infer")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "requestId": "request-123",
                                  "model": "llama3-8b",
                                  "prompt": "Explain dynamic batching",
                                  "maxTokens": 64
                                }
                                """))
                .andExpect(status().isServiceUnavailable())
                .andExpect(header().string(HttpHeaders.RETRY_AFTER, "1"))
                .andExpect(jsonPath("$.code").value("SERVICE_DRAINING"))
                .andExpect(jsonPath("$.requestId").value(REQUEST_ID))
                .andExpect(jsonPath("$.message")
                        .value("Inference server is draining and cannot accept new requests"));
    }
}

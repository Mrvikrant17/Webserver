package com.example.jobs.web;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

public record SubmitRequest(
        @NotBlank String type,
        JsonNode payload,
        @Min(1) @Max(50) Integer maxAttempts,
        String idempotencyKey,
        @Min(0) Long delaySeconds) {}

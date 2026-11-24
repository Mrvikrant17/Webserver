package com.example.jobs.handler;

import java.util.UUID;

public record JobContext(UUID jobId, String type, String payload, int attempt) {}

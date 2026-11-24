package com.example.jobs.handler;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class JobHandlerRegistry {

    private final Map<String, JobHandler> handlers = new HashMap<>();

    public JobHandlerRegistry(List<JobHandler> all) {
        all.forEach(h -> handlers.put(h.type(), h));
    }

    public JobHandler get(String type) {
        JobHandler h = handlers.get(type);
        if (h == null) {
            throw new NonRetryableException("No handler registered for job type '" + type + "'");
        }
        return h;
    }
}

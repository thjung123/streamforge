package com.streamforge.core.launcher;

import java.util.*;

public class JobRegistry {
    private static final Map<String, StreamJob> JOBS = new HashMap<>();

    static {
        ServiceLoader.load(StreamJob.class).forEach(job -> {
            JOBS.put(job.name(), job);
        });
    }

    public static StreamJob get(String name) {
        return JOBS.get(name);
    }

    public static Set<String> listJobs() {
        return JOBS.keySet();
    }
}

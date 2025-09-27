package com.flinkcdc.common.launcher;

import java.util.*;

public class JobRegistry {
    private static final Map<String, FlinkJob> JOBS = new HashMap<>();

    static {
        ServiceLoader.load(FlinkJob.class).forEach(job -> {
            JOBS.put(job.name(), job);
        });
    }

    public static FlinkJob get(String name) {
        return JOBS.get(name);
    }

    public static Set<String> listJobs() {
        return JOBS.keySet();
    }
}

package com.streamforge.core.launcher;

import com.streamforge.core.launcher.StreamJob;
import com.streamforge.core.launcher.JobRegistry;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class JobRegistryTest {

    @BeforeAll
    static void setUp() throws Exception {
        Field jobsField = JobRegistry.class.getDeclaredField("JOBS");
        jobsField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, StreamJob> jobs = (Map<String, StreamJob>) jobsField.get(null);
        jobs.clear();

        jobs.put("dummy-job", new StreamJob() {
            @Override public String name() { return "dummy-job"; }
            @Override public void run(String[] args) { }
        });

        jobs.put("another-job", new StreamJob() {
            @Override public String name() { return "another-job"; }
            @Override public void run(String[] args) { }
        });
    }

    @Test
    @DisplayName("get() should return the correct job instance for a registered name")
    void testGetReturnsJob() {
        StreamJob job = JobRegistry.get("dummy-job");
        assertNotNull(job);
        assertEquals("dummy-job", job.name());
    }

    @Test
    @DisplayName("get() should return null for an unknown job name")
    void testGetReturnsNullForUnknownJob() {
        assertNull(JobRegistry.get("non-existent-job"));
    }

    @Test
    @DisplayName("listJobs() should return all registered job names")
    void testListJobs() {
        Set<String> jobNames = JobRegistry.listJobs();
        assertTrue(jobNames.contains("dummy-job"));
        assertTrue(jobNames.contains("another-job"));
        assertEquals(2, jobNames.size());
    }
}

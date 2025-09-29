package com.flinkcdc.common.launcher;

import com.flinkcdc.common.launcher.FlinkJob;
import com.flinkcdc.common.launcher.JobRegistry;
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
        Map<String, FlinkJob> jobs = (Map<String, FlinkJob>) jobsField.get(null);
        jobs.clear();

        jobs.put("dummy-job", new FlinkJob() {
            @Override public String name() { return "dummy-job"; }
            @Override public void run(String[] args) { }
        });

        jobs.put("another-job", new FlinkJob() {
            @Override public String name() { return "another-job"; }
            @Override public void run(String[] args) { }
        });
    }

    @Test
    @DisplayName("get() should return the correct job instance for a registered name")
    void testGetReturnsJob() {
        FlinkJob job = JobRegistry.get("dummy-job");
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

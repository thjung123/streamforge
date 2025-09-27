package com.flinkcdc;

import com.flinkcdc.common.launcher.FlinkJob;
import com.flinkcdc.common.launcher.JobRegistry;

import java.util.Arrays;

public class App {
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.err.println("Usage: java -jar app.jar <job-name> [args...]");
            System.err.println("Available jobs: " + JobRegistry.listJobs());
            return;
        }

        String jobName = args[0];
        FlinkJob job = JobRegistry.get(jobName);

        if (job == null) {
            throw new IllegalArgumentException("Unknown job: " + jobName +
                    "\nAvailable jobs: " + JobRegistry.listJobs());
        }

        job.run(Arrays.copyOfRange(args, 1, args.length));
    }
}

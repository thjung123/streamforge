package com.streamforge.core.launcher;

import java.util.Arrays;

public final class Launcher {

  private Launcher() {}

  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      System.err.println("Usage: Launcher <job> [args]. Jobs: " + JobRegistry.listJobs());
      System.exit(1);
    }
    StreamJob job = JobRegistry.get(args[0]);
    if (job == null) {
      System.err.println("Unknown job '" + args[0] + "'. Jobs: " + JobRegistry.listJobs());
      System.exit(1);
    }
    job.run(Arrays.copyOfRange(args, 1, args.length));
  }
}

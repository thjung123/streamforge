package com.flinkcdc.common.launcher;

public interface FlinkJob {
    String name();
    void run(String[] args) throws Exception;
}

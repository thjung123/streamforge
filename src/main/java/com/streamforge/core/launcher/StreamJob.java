package com.streamforge.core.launcher;

public interface StreamJob {
    String name();
    void run(String[] args) throws Exception;
}

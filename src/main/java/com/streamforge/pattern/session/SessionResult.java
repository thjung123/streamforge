package com.streamforge.pattern.session;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;

public record SessionResult<R>(
    String key,
    Instant sessionStart,
    Instant sessionEnd,
    int eventCount,
    Duration duration,
    R result)
    implements Serializable {}

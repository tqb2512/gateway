package com.etrade.gateway.domain.entity;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.Data;

@Data
public class BaseEntity implements Cloneable {
    private long createdTime = Instant.now().toEpochMilli();
    private long updatedTime = Instant.now().toEpochMilli();
    private final AtomicInteger version = new AtomicInteger(0);

    private volatile int lastFlushedVersion = -1;

    public void setVersion() {
        version.incrementAndGet();
        updatedTime = Instant.now().toEpochMilli();
    }

    public boolean isDirty() {
        return version.get() != lastFlushedVersion;
    }

    public void markClean() {
        lastFlushedVersion = version.get();
    }

    @Override
    public BaseEntity clone() {
        try {
            return (BaseEntity) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }
}
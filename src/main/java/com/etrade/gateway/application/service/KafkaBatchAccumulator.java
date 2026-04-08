package com.etrade.gateway.application.service;

import com.etrade.gateway.infrastructure.disruptor.BatchFlushQueue;

/**
 * Bridges the Disruptor event-handler thread to the {@link BatchFlushQueue}.
 *
 * <p>Internal buffering and time-based flushing have moved into the
 * {@link com.etrade.gateway.infrastructure.disruptor.BatchFlushWorker} instances
 * managed by {@code BatchFlushQueue}, so this class is now a thin delegation
 * wrapper — kept to avoid changing {@link com.etrade.gateway.infrastructure.disruptor.MarketDataEventHandler}'s
 * dependency surface.
 */
public class KafkaBatchAccumulator {

    private final BatchFlushQueue flushQueue;

    public KafkaBatchAccumulator(BatchFlushQueue flushQueue) {
        this.flushQueue = flushQueue;
    }

    public void add(byte[] encoded) {
        flushQueue.offer(encoded);
    }
}

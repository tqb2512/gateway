package com.etrade.gateway.application.service;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

/**
 * Accumulates encoded market data byte arrays into batches and flushes them
 * to Kafka via {@link KafkaBatchPublishService}.
 *
 * <p>This class is designed to be called from a single Disruptor event handler thread,
 * so it does NOT need to be thread-safe internally. Flushing is triggered by:
 * <ul>
 *   <li>Batch reaching {@code maxBatchSize}</li>
 *   <li>Disruptor {@code endOfBatch} signal</li>
 *   <li>External scheduled timer calling {@link #flush()}</li>
 * </ul>
 */
@Slf4j
public class KafkaBatchAccumulator {

    private final KafkaBatchPublishService batchPublisher;
    private final String topic;
    private final int maxBatchSize;

    private List<byte[]> buffer;

    public KafkaBatchAccumulator(KafkaBatchPublishService batchPublisher, String topic, int maxBatchSize) {
        this.batchPublisher = batchPublisher;
        this.topic = topic;
        this.maxBatchSize = maxBatchSize;
        this.buffer = new ArrayList<>(maxBatchSize);
    }

    /**
     * Add an encoded record to the current batch.
     * Automatically flushes if the batch reaches {@code maxBatchSize}.
     */
    public void add(byte[] encoded) {
        buffer.add(encoded);

        if (buffer.size() >= maxBatchSize) {
            flush();
        }
    }

    /**
     * Flush the current batch to Kafka. If the buffer is empty, this is a no-op.
     */
    public void flush() {
        if (buffer.isEmpty()) {
            return;
        }

        List<byte[]> batch = buffer;
        buffer = new ArrayList<>(maxBatchSize);

        log.info("Flushing batch of {} records to topic [{}]", batch.size(), topic);
        batchPublisher.publishBatch(topic, batch);
    }

    /**
     * @return current number of records in the buffer (for monitoring/testing)
     */
    public int size() {
        return buffer.size();
    }
}

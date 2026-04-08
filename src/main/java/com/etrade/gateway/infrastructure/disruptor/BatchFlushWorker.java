package com.etrade.gateway.infrastructure.disruptor;

import com.etrade.gateway.application.service.KafkaBatchPublishService;
import com.etrade.gateway.infrastructure.monitoring.ThroughputMonitor;
import lombok.extern.slf4j.Slf4j;
import org.jctools.queues.MpmcArrayQueue;

import java.util.ArrayList;
import java.util.List;

/**
 * Dedicated worker thread that polls encoded {@code byte[]} records from a
 * shared {@link MpmcArrayQueue} and publishes them to Kafka in batches.
 *
 * <p>Each worker accumulates records internally up to {@code batchSize}, then
 * flushes. If the queue runs dry before the batch is full, a time-based flush
 * fires after {@code maxWaitMillis} to bound end-to-end latency.
 *
 * <p>Busy-spin is dampened with {@link Thread#yield()} after a configurable
 * number of empty polls, keeping CPU usage acceptable during quiet periods.
 */
@Slf4j
public class BatchFlushWorker implements Runnable {

    private final MpmcArrayQueue<byte[]> queue;
    private final KafkaBatchPublishService batchPublishService;
    private final String topic;
    private final ThroughputMonitor throughputMonitor;
    private final int batchSize;
    private final long maxWaitMillis;

    private volatile boolean running = true;

    public BatchFlushWorker(MpmcArrayQueue<byte[]> queue,
                            KafkaBatchPublishService batchPublishService,
                            String topic,
                            ThroughputMonitor throughputMonitor,
                            int batchSize,
                            long maxWaitMillis) {
        this.queue = queue;
        this.batchPublishService = batchPublishService;
        this.topic = topic;
        this.throughputMonitor = throughputMonitor;
        this.batchSize = batchSize;
        this.maxWaitMillis = maxWaitMillis;
    }

    public void shutdown() {
        running = false;
    }

    @Override
    public void run() {
        log.info("BatchFlushWorker started – draining queue to topic [{}]", topic);

        List<byte[]> batch = new ArrayList<>(batchSize);
        long firstAddTime = 0;
        int emptyPollCount = 0;

        while (running || !queue.isEmpty() || !batch.isEmpty()) {

            byte[] item = queue.poll();
            long now = System.currentTimeMillis();

            if (item != null) {
                emptyPollCount = 0;

                if (batch.isEmpty()) {
                    firstAddTime = now;
                }

                batch.add(item);

                if (batch.size() >= batchSize) {
                    flushBatch(batch);
                    batch.clear();
                    firstAddTime = 0;
                }

                continue;
            }

            // Queue empty → check time-based flush
            if (!batch.isEmpty() && maxWaitMillis > 0 && now - firstAddTime >= maxWaitMillis) {
                flushBatch(batch);
                batch.clear();
                firstAddTime = 0;
                continue;
            }

            // Dampen busy-spin during quiet periods
            emptyPollCount++;
            if (emptyPollCount > 50) {
                emptyPollCount = 0;
                Thread.yield();
            }
        }

        // Flush whatever remains before the thread exits
        if (!batch.isEmpty()) {
            flushBatch(batch);
        }

        log.info("BatchFlushWorker stopped.");
    }

    private void flushBatch(List<byte[]> batch) {
        try {
            batchPublishService.publishBatch(topic, batch);
            throughputMonitor.add("flush-worker-records", batch.size());
            throughputMonitor.increment("flush-worker-batches");
        } catch (Exception e) {
            log.error("BatchFlushWorker publish failed: {}", e.getMessage(), e);
        }
    }
}

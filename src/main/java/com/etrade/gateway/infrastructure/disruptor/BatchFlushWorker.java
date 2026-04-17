package com.etrade.gateway.infrastructure.disruptor;

import com.etrade.gateway.application.service.KafkaBatchPublishService;
import com.etrade.gateway.infrastructure.monitoring.ThroughputMonitor;
import lombok.extern.slf4j.Slf4j;
import org.jctools.queues.MessagePassingQueue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * Dedicated worker thread that polls encoded {@code byte[]} records from a
 * shared {@link MpmcArrayQueue} and publishes them to Kafka in batches.
 *
 * <p>Each worker accumulates records internally up to {@code batchSize}, then
 * flushes. If the queue runs dry before the batch is full, a time-based flush
 * fires after {@code maxWaitMillis} to bound end-to-end latency.
 *
 * <p>Under load the worker runs a tight poll loop; during quiet periods it
 * parks briefly to keep CPU usage low without sacrificing wake-up latency.
 */
@Slf4j
public class BatchFlushWorker implements Runnable {

    private static final long PARK_NANOS = TimeUnit.MICROSECONDS.toNanos(50);
    private static final int SPIN_TRIES = 128;

    private final MessagePassingQueue<byte[]> queue;
    private final KafkaBatchPublishService batchPublishService;
    private final String topic;
    private final ThroughputMonitor throughputMonitor;
    private final int batchSize;
    private final long maxWaitMillis;

    private volatile boolean running = true;

    public BatchFlushWorker(MessagePassingQueue<byte[]> queue,
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

        final List<byte[]> batch = new ArrayList<>(batchSize);
        long firstAddTime = 0L;
        int emptyPollCount = 0;

        while (running || !queue.isEmpty() || !batch.isEmpty()) {
            byte[] item = queue.poll();

            if (item != null) {
                emptyPollCount = 0;
                if (batch.isEmpty()) {
                    firstAddTime = System.currentTimeMillis();
                }
                batch.add(item);
                if (batch.size() >= batchSize) {
                    flushBatch(batch);
                    batch.clear();
                    firstAddTime = 0L;
                }
                continue;
            }

            // Queue empty → honour time-based flush bound
            if (!batch.isEmpty() && maxWaitMillis > 0
                    && System.currentTimeMillis() - firstAddTime >= maxWaitMillis) {
                flushBatch(batch);
                batch.clear();
                firstAddTime = 0L;
                continue;
            }

            // Staircase back-off: spin, yield, then park — keeps wake-up
            // latency in the microsecond range without burning a core.
            emptyPollCount++;
            if (emptyPollCount < SPIN_TRIES) {
                Thread.onSpinWait();
            } else if (emptyPollCount < SPIN_TRIES * 2) {
                Thread.yield();
            } else {
                LockSupport.parkNanos(PARK_NANOS);
            }
        }

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

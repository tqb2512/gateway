package com.etrade.gateway.infrastructure.disruptor;

import com.etrade.gateway.application.service.KafkaBatchPublishService;
import com.etrade.gateway.infrastructure.monitoring.ThroughputMonitor;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.jctools.queues.MpmcArrayQueue;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Shared lock-free queue that decouples the Disruptor event-handler thread
 * from Kafka I/O.
 *
 * <p>Encoded {@code byte[]} records are deposited via {@link #offer(byte[])} by
 * the Disruptor pipeline and drained by a fixed pool of {@link BatchFlushWorker}
 * threads. Each worker accumulates records into batches internally, flushing on
 * either a full batch or a configurable time-out — identical to the
 * {@code TranslogQueue / TransLogWorker} pattern.
 *
 * <p>Lifecycle is managed by Spring: the worker pool is started in the
 * constructor and shut down gracefully on {@link #shutdown()}.
 */
@Slf4j
@Service
public class BatchFlushQueue {

    private final MpmcArrayQueue<byte[]> sharedQueue;
    private final ExecutorService executor;
    private final BatchFlushWorker[] workers;

    public BatchFlushQueue(
            KafkaBatchPublishService batchPublishService,
            ThroughputMonitor throughputMonitor,
            @Value("${kafka.topic.market-quote}") String topic,
            @Value("${kafka.batch.max-size:200000}") int batchSize,
            @Value("${kafka.batch.flush-interval-ms:10000}") long flushIntervalMs,
            @Value("${kafka.batch.worker-count:2}") int workerCount,
            @Value("${kafka.batch.queue-capacity:1000000}") int queueCapacity) {

        this.sharedQueue = new MpmcArrayQueue<>(queueCapacity);
        this.executor = Executors.newFixedThreadPool(workerCount);
        this.workers = new BatchFlushWorker[workerCount];

        for (int i = 0; i < workerCount; i++) {
            BatchFlushWorker worker = new BatchFlushWorker(
                    sharedQueue, batchPublishService, topic, throughputMonitor, batchSize, flushIntervalMs);
            workers[i] = worker;
            executor.submit(worker);
        }

        log.info("BatchFlushQueue started: workers={}, queueCapacity={}, batchSize={}, flushIntervalMs={}",
                workerCount, queueCapacity, batchSize, flushIntervalMs);
    }

    /**
     * Enqueue a single encoded record. Spins (yielding) only if the queue is
     * momentarily full — in practice this should never block under normal load
     * given the large default queue capacity.
     */
    public void offer(byte[] encoded) {
        while (!sharedQueue.offer(encoded)) {
            Thread.yield();
        }
    }

    @PreDestroy
    public void shutdown() {
        log.info("[BatchFlushQueue] Shutdown requested...");

        for (BatchFlushWorker w : workers) {
            w.shutdown();
        }

        executor.shutdown();

        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                log.warn("[BatchFlushQueue] Force shutting down workers...");
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        log.info("[BatchFlushQueue] Stopped cleanly.");
    }
}

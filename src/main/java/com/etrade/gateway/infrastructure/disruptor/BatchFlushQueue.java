package com.etrade.gateway.infrastructure.disruptor;

import com.etrade.gateway.application.service.KafkaBatchPublishService;
import com.etrade.gateway.infrastructure.monitoring.ThroughputMonitor;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.jctools.queues.MessagePassingQueue;
import org.jctools.queues.SpmcArrayQueue;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

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

    private final SpmcArrayQueue<byte[]> sharedQueue;
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

        // SPMC: the sole producer is the Disruptor event-handler thread;
        // {@code workerCount} consumers drain the queue in parallel. Using
        // SPMC instead of MPMC removes the producer-side CAS contention that
        // would otherwise cap Disruptor hand-off throughput.
        this.sharedQueue = new SpmcArrayQueue<>(queueCapacity);
        this.executor = Executors.newFixedThreadPool(workerCount, r -> {
            Thread t = new Thread(r, "batch-flush-worker");
            t.setDaemon(true);
            return t;
        });
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
     * Enqueue a single encoded record. Invoked exclusively from the Disruptor
     * event-handler thread (single producer). Parks briefly if the queue is
     * full rather than burning CPU.
     */
    public void offer(byte[] encoded) {
        if (sharedQueue.offer(encoded)) {
            return;
        }
        // Slow path: queue full — back off without hot-spinning.
        final MessagePassingQueue<byte[]> q = sharedQueue;
        while (!q.offer(encoded)) {
            LockSupport.parkNanos(1_000L);
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

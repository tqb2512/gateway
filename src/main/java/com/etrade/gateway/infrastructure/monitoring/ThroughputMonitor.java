package com.etrade.gateway.infrastructure.monitoring;

import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * Lightweight throughput monitor that tracks requests-per-second
 * for multiple named counters.
 *
 * <p>Usage: inject this bean and call {@link #increment(String)} on the
 * hot path. The monitor logs the computed rate every
 * {@code monitoring.throughput.interval-seconds} seconds (default 5).
 *
 * <p>Counters are created lazily on first use — no up-front registration needed.
 */
@Slf4j
@Component
public class ThroughputMonitor {

    private final Map<String, LongAdder> counters = new ConcurrentHashMap<>();
    private final Map<String, LongAdder> snapshots = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler;
    private final long intervalSeconds;

    public ThroughputMonitor(
            @Value("${monitoring.throughput.interval-seconds:5}") long intervalSeconds) {
        this.intervalSeconds = intervalSeconds;

        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "throughput-monitor");
            t.setDaemon(true);
            return t;
        });

        this.scheduler.scheduleAtFixedRate(this::report, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
        log.info("ThroughputMonitor started: reporting every {}s", intervalSeconds);
    }

    /**
     * Increment the counter for the given metric name by 1.
     * This is designed to be called on the hot path — uses {@link LongAdder}
     * for minimal contention.
     */
    public void increment(String name) {
        counters.computeIfAbsent(name, k -> new LongAdder()).increment();
    }

    /**
     * Increment the counter for the given metric name by the specified amount.
     */
    public void add(String name, long count) {
        counters.computeIfAbsent(name, k -> new LongAdder()).add(count);
    }

    /**
     * Get the current total count for a given metric (since application start).
     */
    public long getCount(String name) {
        LongAdder adder = counters.get(name);
        return adder != null ? adder.sum() : 0;
    }

    private void report() {
        if (counters.isEmpty()) {
            return;
        }

        StringBuilder sb = new StringBuilder();
        sb.append("Throughput [interval=").append(intervalSeconds).append("s]:");

        for (Map.Entry<String, LongAdder> entry : counters.entrySet()) {
            String name = entry.getKey();
            long currentTotal = entry.getValue().sum();

            // Get the previous snapshot
            LongAdder prev = snapshots.computeIfAbsent(name, k -> new LongAdder());
            long previousTotal = prev.sum();

            long delta = currentTotal - previousTotal;
            double rps = (double) delta / intervalSeconds;

            sb.append(String.format(" | %s: %,.1f req/s (total: %,d)", name, rps, currentTotal));

            // Update snapshot to current total
            prev.reset();
            prev.add(currentTotal);
        }

        log.info("{}", sb);
    }

    @PreDestroy
    public void shutdown() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(2, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        log.info("ThroughputMonitor shut down.");
    }
}

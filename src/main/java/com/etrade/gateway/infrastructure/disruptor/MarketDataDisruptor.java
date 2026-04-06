package com.etrade.gateway.infrastructure.disruptor;

import com.etrade.gateway.application.service.KafkaBatchAccumulator;
import com.etrade.gateway.application.service.KafkaBatchPublishService;
import com.etrade.gateway.application.service.ProcessMarketEncode;
import com.etrade.gateway.application.service.ProcessMarketParse;
import com.etrade.gateway.domain.service.PublishService;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Self-contained Disruptor wrapper for market data processing.
 *
 * <p>Pipeline: WebSocket → RingBuffer → Parse → Encode → BatchAccumulate → Kafka
 *
 * <p>Implements {@link PublishService} so that producers (e.g. WebSocket handler)
 * only depend on the abstract publish interface, not the LMAX Disruptor API.
 */
@Slf4j
@Service("MarketDataDisruptor")
public class MarketDataDisruptor implements PublishService<Void, String> {

    private final Disruptor<MarketDataEvent> disruptor;
    private final RingBuffer<MarketDataEvent> ringBuffer;
    private final ScheduledExecutorService flushScheduler;

    public MarketDataDisruptor(
            ProcessMarketParse parser,
            ProcessMarketEncode encoder,
            KafkaBatchPublishService batchPublishService,
            @Value("${disruptor.ring-buffer-size:1024}") int ringBufferSize,
            @Value("${kafka.topic.market-quote}") String kafkaTopic,
            @Value("${kafka.batch.max-size:64}") int maxBatchSize,
            @Value("${kafka.batch.flush-interval-ms:50}") long flushIntervalMs) {

        // Build the batch accumulator
        KafkaBatchAccumulator accumulator = new KafkaBatchAccumulator(batchPublishService, kafkaTopic, maxBatchSize);

        // Build the event handler
        MarketDataEventHandler eventHandler = new MarketDataEventHandler(parser, encoder, accumulator);

        // Build & start the disruptor
        disruptor = new Disruptor<>(
                new MarketDataEvent.Factory(),
                ringBufferSize,
                Thread.ofPlatform().name("market-data-").factory(),
                ProducerType.SINGLE,
                new SleepingWaitStrategy());

        disruptor.handleEventsWith(eventHandler);

        disruptor.setDefaultExceptionHandler(new com.lmax.disruptor.ExceptionHandler<MarketDataEvent>() {
            @Override
            public void handleEventException(Throwable ex, long sequence, MarketDataEvent event) {
                log.error("Disruptor event exception at seq={}: {}", sequence, ex.getMessage(), ex);
            }

            @Override
            public void handleOnStartException(Throwable ex) {
                log.error("Disruptor start exception: {}", ex.getMessage(), ex);
            }

            @Override
            public void handleOnShutdownException(Throwable ex) {
                log.error("Disruptor shutdown exception: {}", ex.getMessage(), ex);
            }
        });

        disruptor.start();
        ringBuffer = disruptor.getRingBuffer();

        // Scheduled flush for time-based batching
        flushScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "batch-flush-scheduler");
            t.setDaemon(true);
            return t;
        });

        flushScheduler.scheduleAtFixedRate(() -> {
            try {
                accumulator.flush();
            } catch (Exception e) {
                log.error("Scheduled batch flush failed: {}", e.getMessage(), e);
            }
        }, flushIntervalMs, flushIntervalMs, TimeUnit.MILLISECONDS);

        log.info("MarketDataDisruptor started: ringBufferSize={}, batchSize={}, flushIntervalMs={}",
                ringBufferSize, maxBatchSize, flushIntervalMs);
    }

    @Override
    public Void publish(String channel, String rawJson) {
        long sequence = ringBuffer.next();
        try {
            MarketDataEvent event = ringBuffer.get(sequence);
            event.setRawJson(rawJson);
        } finally {
            ringBuffer.publish(sequence);
        }
        return null;
    }

    @PreDestroy
    public void shutdown() {
        log.info("Shutting down MarketDataDisruptor...");

        if (flushScheduler != null) {
            flushScheduler.shutdown();
            try {
                if (!flushScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    flushScheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                flushScheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        if (disruptor != null) {
            disruptor.shutdown();
        }

        log.info("MarketDataDisruptor shut down.");
    }
}

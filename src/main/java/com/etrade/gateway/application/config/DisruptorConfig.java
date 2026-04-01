package com.etrade.gateway.application.config;

import com.etrade.gateway.application.disruptor.MarketDataEvent;
import com.etrade.gateway.application.disruptor.MarketDataEventFactory;
import com.etrade.gateway.application.disruptor.MarketDataEventHandler;
import com.etrade.gateway.application.service.KafkaBatchAccumulator;
import com.etrade.gateway.application.service.KafkaBatchPublishService;
import com.etrade.gateway.application.service.ProcessMarketEncode;
import com.etrade.gateway.application.service.ProcessMarketParse;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Configures the LMAX Disruptor pipeline for market data processing.
 *
 * <p>Pipeline: WebSocket → RingBuffer → Parse → Encode → BatchAccumulate → Kafka
 */
@Slf4j
@Configuration
public class DisruptorConfig {

    @Value("${disruptor.ring-buffer-size:1024}")
    private int ringBufferSize;

    @Value("${kafka.topic.market-quote}")
    private String kafkaTopic;

    @Value("${kafka.batch.max-size:64}")
    private int maxBatchSize;

    @Value("${kafka.batch.flush-interval-ms:50}")
    private long flushIntervalMs;

    private Disruptor<MarketDataEvent> disruptor;
    private ScheduledExecutorService flushScheduler;

    @Bean
    public KafkaBatchAccumulator kafkaBatchAccumulator(KafkaBatchPublishService batchPublishService) {
        return new KafkaBatchAccumulator(batchPublishService, kafkaTopic, maxBatchSize);
    }

    @Bean
    public MarketDataEventHandler marketDataEventHandler(
            ProcessMarketParse parser,
            ProcessMarketEncode encoder,
            KafkaBatchAccumulator accumulator) {
        return new MarketDataEventHandler(parser, encoder, accumulator);
    }

    @Bean
    public Disruptor<MarketDataEvent> marketDataDisruptor(MarketDataEventHandler eventHandler) {
        disruptor = new Disruptor<>(
                new MarketDataEventFactory(),
                ringBufferSize,
                DaemonThreadFactory.INSTANCE,
                ProducerType.SINGLE,
                new SleepingWaitStrategy()
        );

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
        log.info("Disruptor started: ringBufferSize={}, batchSize={}, flushIntervalMs={}",
                ringBufferSize, maxBatchSize, flushIntervalMs);

        return disruptor;
    }

    @Bean
    public ScheduledExecutorService batchFlushScheduler(KafkaBatchAccumulator accumulator) {
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

        log.info("Batch flush scheduler started: interval={}ms", flushIntervalMs);
        return flushScheduler;
    }

    @PreDestroy
    public void shutdown() {
        log.info("Shutting down Disruptor and flush scheduler...");

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

        log.info("Disruptor and flush scheduler shut down.");
    }
}

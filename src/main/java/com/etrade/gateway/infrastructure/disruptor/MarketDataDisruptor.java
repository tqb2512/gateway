package com.etrade.gateway.infrastructure.disruptor;

import com.etrade.gateway.application.service.KafkaBatchAccumulator;
import com.etrade.gateway.application.service.ProcessMarketEncode;
import com.etrade.gateway.application.service.ProcessMarketParse;
import com.etrade.gateway.domain.service.PublishService;
import com.etrade.gateway.infrastructure.monitoring.ThroughputMonitor;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * Self-contained Disruptor wrapper for market data processing.
 *
 * <p>Pipeline: WebSocket → RingBuffer → Parse → Encode → BatchFlushQueue → Workers → Kafka
 *
 * <p>The {@link BatchFlushQueue} owns the worker pool and the shared
 * {@link org.jctools.queues.MpmcArrayQueue}. Its lifecycle is managed by
 * Spring ({@code @PreDestroy}), so this class only needs to shut down the
 * Disruptor itself.
 *
 * <p>Implements {@link PublishService} so that producers (e.g. the WebSocket
 * handler) depend only on the abstract publish interface.
 */
@Slf4j
@Service("MarketDataDisruptor")
public class MarketDataDisruptor implements PublishService<Void, String> {

    private final Disruptor<MarketDataEvent> disruptor;
    private final RingBuffer<MarketDataEvent> ringBuffer;
    private final ThroughputMonitor throughputMonitor;

    public MarketDataDisruptor(
            ProcessMarketParse parser,
            ProcessMarketEncode encoder,
            BatchFlushQueue batchFlushQueue,
            ThroughputMonitor throughputMonitor,
            @Value("${disruptor.ring-buffer-size:1024}") int ringBufferSize) {

        this.throughputMonitor = throughputMonitor;

        KafkaBatchAccumulator accumulator = new KafkaBatchAccumulator(batchFlushQueue);

        MarketDataEventHandler eventHandler = new MarketDataEventHandler(parser, encoder, accumulator, throughputMonitor);

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

        log.info("MarketDataDisruptor started: ringBufferSize={}", ringBufferSize);
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
        throughputMonitor.increment("disruptor-publish");
        return null;
    }

    @PreDestroy
    public void shutdown() {
        log.info("Shutting down MarketDataDisruptor...");
        if (disruptor != null) {
            disruptor.shutdown();
        }
        log.info("MarketDataDisruptor shut down.");
    }
}

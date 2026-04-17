package com.etrade.gateway.infrastructure.disruptor;

import com.etrade.gateway.application.service.KafkaBatchAccumulator;
import com.etrade.gateway.application.service.ProcessMarketEncode;
import com.etrade.gateway.application.service.ProcessMarketParse;
import com.etrade.gateway.domain.entity.QuoteEntity;
import com.etrade.gateway.infrastructure.monitoring.ThroughputMonitor;
import com.lmax.disruptor.EventHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Disruptor event handler that processes each market data event:
 * 1. Parse pre-decoded JsonNode → QuoteEntity
 * 2. Encode QuoteEntity → byte[]
 * 3. Enqueue encoded bytes onto the batch flush queue
 *
 * <p>This class runs on a single Disruptor worker thread, so the parser and
 * encoder are invoked without synchronisation.
 */
@Slf4j
@RequiredArgsConstructor
public class MarketDataEventHandler implements EventHandler<MarketDataEvent> {

    private final ProcessMarketParse parser;
    private final ProcessMarketEncode encoder;
    private final KafkaBatchAccumulator batchAccumulator;
    private final ThroughputMonitor throughputMonitor;

    @Override
    public void onEvent(MarketDataEvent event, long sequence, boolean endOfBatch) {
        try {
            QuoteEntity quote = parser.process(event.getPayload());
            byte[] encoded = encoder.process(quote);
            batchAccumulator.add(encoded);
            throughputMonitor.increment("disruptor-processed");
        } catch (Exception e) {
            log.error("Failed to process market data event seq={}: {}", sequence, e.getMessage(), e);
        } finally {
            event.clear();
        }
    }
}

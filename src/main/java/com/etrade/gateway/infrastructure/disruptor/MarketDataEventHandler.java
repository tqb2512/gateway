package com.etrade.gateway.infrastructure.disruptor;

import com.etrade.gateway.application.service.KafkaBatchAccumulator;
import com.etrade.gateway.application.service.ProcessMarketEncode;
import com.etrade.gateway.application.service.ProcessMarketParse;
import com.etrade.gateway.domain.entity.QuoteEntity;
import com.lmax.disruptor.EventHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Disruptor event handler that processes each market data event:
 * 1. Parse raw JSON → QuoteEntity
 * 2. Encode QuoteEntity → byte[]
 * 3. Add encoded bytes to the batch accumulator
 *
 * Uses the {@code endOfBatch} flag to trigger a flush at the Disruptor batch boundary.
 */
@Slf4j
@RequiredArgsConstructor
public class MarketDataEventHandler implements EventHandler<MarketDataEvent> {

    private final ProcessMarketParse parser;
    private final ProcessMarketEncode encoder;
    private final KafkaBatchAccumulator batchAccumulator;

    @Override
    public void onEvent(MarketDataEvent event, long sequence, boolean endOfBatch) {
        try {
            // Step 1: Parse
            QuoteEntity quote = parser.process(event.getRawJson());
            event.setQuote(quote);

            log.debug("Parsed quote seq={}: {}/{} bid={} ask={}",
                    sequence, quote.getBaseCurrency(), quote.getQuoteCurrency(),
                    quote.getBid(), quote.getAsk());

            // Step 2: Encode
            byte[] encoded = encoder.process(quote);
            event.setEncoded(encoded);

            // Step 3: Add to batch
            batchAccumulator.add(encoded);

            // Flush if this is the end of a Disruptor batch
            if (endOfBatch) {
                batchAccumulator.flush();
            }
        } catch (Exception e) {
            log.error("Failed to process market data event seq={}: {}", sequence, e.getMessage(), e);
        } finally {
            event.clear();
        }
    }
}

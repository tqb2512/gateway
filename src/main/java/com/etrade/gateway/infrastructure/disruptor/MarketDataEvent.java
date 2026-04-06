package com.etrade.gateway.infrastructure.disruptor;

import com.etrade.gateway.domain.entity.QuoteEntity;
import com.lmax.disruptor.EventFactory;

/**
 * Mutable event object held in the Disruptor ring buffer.
 * Pre-allocated and reused — fields are set per-event and cleared after processing.
 */
public class MarketDataEvent {

    private String rawJson;
    private QuoteEntity quote;
    private byte[] encoded;

    public String getRawJson() {
        return rawJson;
    }

    public void setRawJson(String rawJson) {
        this.rawJson = rawJson;
    }

    public QuoteEntity getQuote() {
        return quote;
    }

    public void setQuote(QuoteEntity quote) {
        this.quote = quote;
    }

    public byte[] getEncoded() {
        return encoded;
    }

    public void setEncoded(byte[] encoded) {
        this.encoded = encoded;
    }

    /**
     * Clear all fields for reuse in the ring buffer.
     */
    public void clear() {
        this.rawJson = null;
        this.quote = null;
        this.encoded = null;
    }

    public static class Factory implements EventFactory<MarketDataEvent> {
        @Override
        public MarketDataEvent newInstance() {
            return new MarketDataEvent();
        }
    }
}

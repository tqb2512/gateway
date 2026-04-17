package com.etrade.gateway.infrastructure.disruptor;

import com.fasterxml.jackson.databind.JsonNode;
import com.lmax.disruptor.EventFactory;

/**
 * Mutable event object held in the Disruptor ring buffer.
 * Pre-allocated and reused — fields are set per-event and cleared after processing.
 */
public class MarketDataEvent {

    private JsonNode payload;

    public JsonNode getPayload() {
        return payload;
    }

    public void setPayload(JsonNode payload) {
        this.payload = payload;
    }

    /**
     * Clear the payload reference so the Disruptor does not pin it until the
     * slot is next overwritten.
     */
    public void clear() {
        this.payload = null;
    }

    public static class Factory implements EventFactory<MarketDataEvent> {
        @Override
        public MarketDataEvent newInstance() {
            return new MarketDataEvent();
        }
    }
}

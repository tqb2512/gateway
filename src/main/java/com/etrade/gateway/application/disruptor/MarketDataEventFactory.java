package com.etrade.gateway.application.disruptor;

import com.lmax.disruptor.EventFactory;

/**
 * Factory to pre-allocate {@link MarketDataEvent} instances in the Disruptor ring buffer.
 */
public class MarketDataEventFactory implements EventFactory<MarketDataEvent> {

    @Override
    public MarketDataEvent newInstance() {
        return new MarketDataEvent();
    }
}

package com.etrade.gateway.domain.service;

import com.etrade.gateway.domain.entity.QuoteEntity;

@FunctionalInterface
public interface ProcessMarketEventService<T> {
    void process(QuoteEntity data);
}

package com.etrade.gateway.domain.service;

import com.etrade.gateway.domain.entity.QuoteEntity;

@FunctionalInterface
public interface ProcessMarketEncodeService<T> {
    T process(QuoteEntity data);
}

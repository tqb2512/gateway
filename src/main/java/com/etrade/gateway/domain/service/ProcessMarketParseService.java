package com.etrade.gateway.domain.service;

import com.etrade.gateway.domain.entity.QuoteEntity;

@FunctionalInterface
public interface ProcessMarketParseService<T> {
    QuoteEntity process(T data);
}

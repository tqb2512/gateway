package com.etrade.gateway.domain.service;

@FunctionalInterface
public interface ProcessMarketReceiveService<T> {
    void process(T data);
}

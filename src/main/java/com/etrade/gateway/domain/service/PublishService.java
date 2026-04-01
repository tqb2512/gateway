package com.etrade.gateway.domain.service;

@FunctionalInterface
public interface PublishService<T, D> {
    T publish(String channel, D data);
}

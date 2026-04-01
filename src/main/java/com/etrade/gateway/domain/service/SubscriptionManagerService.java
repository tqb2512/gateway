package com.etrade.gateway.domain.service;

import com.etrade.gateway.domain.entity.CurrencyPairSubscription;

public interface SubscriptionManagerService {

    void addPair(CurrencyPairSubscription.CurrencyPair pair);

    CurrencyPairSubscription getSubscription();
}

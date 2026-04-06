package com.etrade.gateway.domain.service;

import com.etrade.gateway.domain.entity.CurrencyPairSubscription;
import com.etrade.gateway.presentation.dto.SubscribeResponse;

import java.util.List;

public interface SubscriptionManagerService {

    void addPair(CurrencyPairSubscription.CurrencyPair pair);

    SubscribeResponse subscribe(List<CurrencyPairSubscription.CurrencyPair> pairs);

    CurrencyPairSubscription getSubscription();
}

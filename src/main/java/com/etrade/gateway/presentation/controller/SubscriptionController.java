package com.etrade.gateway.presentation.controller;

import com.etrade.gateway.domain.entity.CurrencyPairSubscription;
import com.etrade.gateway.domain.service.SubscriptionManagerService;
import com.etrade.gateway.presentation.dto.SubscribeRequest;
import com.etrade.gateway.presentation.dto.SubscribeResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@RestController
@RequestMapping("/api/v1/subscription")
@RequiredArgsConstructor
public class SubscriptionController {

    private final SubscriptionManagerService subscriptionManagerService;

    @PostMapping("/subscribe")
    public ResponseEntity<SubscribeResponse> subscribe(@RequestBody SubscribeRequest request) {
        log.info("Received subscribe request with {} currency pairs", request.getCurrencyPairList().size());

        List<CurrencyPairSubscription.CurrencyPair> pairs = request.getCurrencyPairList().stream()
                .map(item -> CurrencyPairSubscription.CurrencyPair.builder()
                        .buyCurrency(item.getBuyCurrency())
                        .sellCurrency(item.getSellCurrency())
                        .tenor(item.getTenor())
                        .build())
                .collect(Collectors.toList());

        SubscribeResponse response = subscriptionManagerService.subscribe(pairs);
        return ResponseEntity.ok(response);
    }

    @PostMapping("/unsubscribe")
    public ResponseEntity<SubscribeResponse> unsubscribe(@RequestBody SubscribeRequest request) {
        log.info("Received unsubscribe request with {} currency pairs", request.getCurrencyPairList().size());

        List<CurrencyPairSubscription.CurrencyPair> pairs = request.getCurrencyPairList().stream()
                .map(item -> CurrencyPairSubscription.CurrencyPair.builder()
                        .buyCurrency(item.getBuyCurrency())
                        .sellCurrency(item.getSellCurrency())
                        .tenor(item.getTenor())
                        .build())
                .collect(Collectors.toList());

        SubscribeResponse response = subscriptionManagerService.unsubscribe(pairs);
        return ResponseEntity.ok(response);
    }

    @GetMapping("/pairs")
    public ResponseEntity<CurrencyPairSubscription> getSubscribedPairs() {
        return ResponseEntity.ok(subscriptionManagerService.getSubscription());
    }
}

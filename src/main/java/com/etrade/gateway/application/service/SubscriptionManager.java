package com.etrade.gateway.application.service;

import com.etrade.gateway.domain.entity.CurrencyPairSubscription;
import com.etrade.gateway.domain.service.SubscriptionManagerService;
import com.etrade.gateway.presentation.dto.SubscribeResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

@Slf4j
@Service
public class SubscriptionManager implements SubscriptionManagerService {

    private final RestClient restClient;
    private final ObjectMapper objectMapper;
    private final CurrencyPairSubscription subscription;

    @Value("${scb.subscription.url}")
    private String subscriptionUrl;

    @Value("${scb.client-id}")
    private String clientId;

    @Value("${scb.rate-category-id}")
    private String rateCategoryId;

    public SubscriptionManager(RestClient restClient, ObjectMapper objectMapper) {
        this.restClient = restClient;
        this.objectMapper = objectMapper;
        this.subscription = CurrencyPairSubscription.builder()
                .pairList(new CopyOnWriteArrayList<>())
                .build();
    }

    @Override
    public void addPair(CurrencyPairSubscription.CurrencyPair pair) {
        subscription.getPairList().add(pair);
        subscription.setClientId(clientId);
        subscription.setRateCategoryId(rateCategoryId);
        log.info("Added currency pair: {}/{} tenor={}",
                pair.getBuyCurrency(), pair.getSellCurrency(), pair.getTenor());

        sendSubscriptionRequest(pair);
    }

    @Override
    public SubscribeResponse subscribe(List<CurrencyPairSubscription.CurrencyPair> pairs) {
        String url = subscriptionUrl
                .replace("{clientId}", clientId)
                .replace("{rateCatId}", rateCategoryId);

        try {
            List<Map<String, String>> pairListBody = pairs.stream()
                    .map(p -> Map.of(
                            "buyCurrency", p.getBuyCurrency(),
                            "sellCurrency", p.getSellCurrency(),
                            "tenor", p.getTenor()
                    ))
                    .collect(Collectors.toList());

            Map<String, Object> requestBody = Map.of("currencyPairList", pairListBody);
            String body = objectMapper.writeValueAsString(requestBody);
            log.info("Sending subscribe request to {}: {}", url, body);

            SubscribeResponse response = restClient.post()
                    .uri(url)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(body)
                    .retrieve()
                    .body(SubscribeResponse.class);

            log.info("Subscribe response: status={}", response != null ? response.getStatus() : "null");

            if (response != null && ("SUCCESS".equals(response.getStatus())
                    || "PARTIAL_SUCCESS".equals(response.getStatus()))) {

                subscription.setClientId(clientId);
                subscription.setRateCategoryId(rateCategoryId);

                for (CurrencyPairSubscription.CurrencyPair pair : pairs) {
                    boolean alreadyExists = subscription.getPairList().stream()
                            .anyMatch(existing ->
                                    existing.getBuyCurrency().equals(pair.getBuyCurrency())
                                            && existing.getSellCurrency().equals(pair.getSellCurrency())
                                            && existing.getTenor().equals(pair.getTenor()));

                    if (!alreadyExists) {
                        subscription.getPairList().add(pair);
                        log.info("Subscribed currency pair added to pairList: {}/{} tenor={}",
                                pair.getBuyCurrency(), pair.getSellCurrency(), pair.getTenor());
                    }
                }
            }

            return response;
        } catch (Exception e) {
            log.error("Failed to send subscribe request: {}", e.getMessage(), e);
            throw new RuntimeException("Subscription request failed: " + e.getMessage(), e);
        }
    }

    @Override
    public CurrencyPairSubscription getSubscription() {
        return subscription;
    }

    private void sendSubscriptionRequest(CurrencyPairSubscription.CurrencyPair pair) {
        String url = subscriptionUrl
                .replace("{clientId}", clientId)
                .replace("{rateCatId}", rateCategoryId);

        try {
            Map<String, Object> requestBody = Map.of(
                    "currencyPairList", Collections.singletonList(Map.of(
                            "buyCurrency", pair.getBuyCurrency(),
                            "sellCurrency", pair.getSellCurrency(),
                            "tenor", pair.getTenor()
                    ))
            );

            String body = objectMapper.writeValueAsString(requestBody);
            log.info("Sending subscription request to {}: {}", url, body);

            String response = restClient.post()
                    .uri(url)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(body)
                    .retrieve()
                    .body(String.class);

            log.info("Subscription response: {}", response);
        } catch (Exception e) {
            log.error("Failed to send subscription request for {}/{}: {}",
                    pair.getBuyCurrency(), pair.getSellCurrency(), e.getMessage(), e);
        }
    }
}

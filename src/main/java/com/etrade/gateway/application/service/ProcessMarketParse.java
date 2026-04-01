package com.etrade.gateway.application.service;

import com.etrade.gateway.domain.entity.QuoteEntity;
import com.etrade.gateway.domain.service.ProcessMarketParseService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class ProcessMarketParse implements ProcessMarketParseService<String> {

    private final ObjectMapper objectMapper;

    @Override
    public QuoteEntity process(String data) {
        try {
            JsonNode root = objectMapper.readTree(data);

            double bid = parseRate(root, "bid");
            double ask = parseRate(root, "ask");

            return QuoteEntity.builder()
                    .rateType(root.path("rateType").asText())
                    .rateQuoteID(root.path("rateQuoteID").asText())
                    .rateCategoryID(root.path("rateCategoryID").asText())
                    .validFrom(root.path("validFrom").asLong())
                    .validTill(root.path("validTill").asLong())
                    .baseCurrency(root.path("baseCurrency").asText())
                    .quoteCurrency(root.path("quoteCurrency").asText())
                    .tenor(root.path("tenor").asText())
                    .status(root.path("status").asText())
                    .bid(bid)
                    .ask(ask)
                    .valid(root.path("valid").asBoolean())
                    .build();
        } catch (Exception e) {
            log.error("Failed to parse market data: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to parse market data", e);
        }
    }

    private double parseRate(JsonNode root, String field) {
        JsonNode node = root.path(field);
        if (node.isObject()) {
            return Double.parseDouble(node.path("rate").asText());
        }
        return node.asDouble();
    }
}

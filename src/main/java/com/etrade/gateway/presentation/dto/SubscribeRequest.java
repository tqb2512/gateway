package com.etrade.gateway.presentation.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SubscribeRequest {

    private List<CurrencyPairItem> currencyPairList;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CurrencyPairItem {
        private String buyCurrency;
        private String sellCurrency;
        private String tenor;
    }
}

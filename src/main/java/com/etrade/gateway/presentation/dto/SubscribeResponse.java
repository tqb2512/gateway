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
public class SubscribeResponse {

    private String status;
    private List<CurrencyPairStatus> currencyPairList;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CurrencyPairStatus {
        private String status;
        private String buyCurrency;
        private String sellCurrency;
        private String tenor;
    }
}

package com.etrade.gateway.domain.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class CurrencyPairSubscription extends BaseEntity {

    private String clientId;
    private String rateCategoryId;
    private List<CurrencyPair> pairList;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CurrencyPair {

        private String buyCurrency;
        private String sellCurrency;
        private String tenor;
    }
}

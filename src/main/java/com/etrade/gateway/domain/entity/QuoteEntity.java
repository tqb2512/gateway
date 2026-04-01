package com.etrade.gateway.domain.entity;

import lombok.Builder;
import lombok.Data;
import lombok.With;

@Data
@Builder
@With
public class QuoteEntity {
    private String rateType;
    private String rateQuoteID;
    private String rateCategoryID;
    private long validFrom;
    private long validTill;
    private String baseCurrency;
    private String quoteCurrency;
    private String tenor;
    private String status;
    private double bid;
    private double ask;
    private Boolean valid;
}

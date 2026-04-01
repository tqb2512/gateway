package com.etrade.gateway.application.service;

import com.etrade.gateway.domain.entity.QuoteEntity;
import com.etrade.gateway.domain.service.ProcessMarketEncodeService;
import com.etrade.gateway.sbe.BooleanType;
import com.etrade.gateway.sbe.MessageHeaderEncoder;
import com.etrade.gateway.sbe.QuoteEncoder;
import org.agrona.concurrent.UnsafeBuffer;
import org.springframework.stereotype.Service;

import java.nio.ByteBuffer;

@Service
public class ProcessMarketEncode implements ProcessMarketEncodeService<byte[]> {

    private static final int BUFFER_SIZE = 223;

    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final QuoteEncoder quoteEncoder = new QuoteEncoder();

    @Override
    public byte[] process(QuoteEntity data) {
        final ByteBuffer byteBuffer = ByteBuffer.allocate(BUFFER_SIZE);
        final UnsafeBuffer buffer = new UnsafeBuffer(byteBuffer);

        quoteEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder);

        // Fixed-length fields
        quoteEncoder
                .bid(data.getBid())
                .ask(data.getAsk())
                .valid(mapBoolean(data.getValid()))
                .validFrom(data.getValidFrom())
                .validTill(data.getValidTill());

        // Variable-length fields (must be encoded in schema-defined order)
        quoteEncoder
                .rateType(data.getRateType())
                .rateQuoteID(data.getRateQuoteID())
                .rateCategoryID(data.getRateCategoryID())
                .baseCurrency(data.getBaseCurrency())
                .quoteCurrency(data.getQuoteCurrency())
                .tenor(data.getTenor())
                .status(data.getStatus());

        final int encodedLength = MessageHeaderEncoder.ENCODED_LENGTH + quoteEncoder.encodedLength();
        final byte[] encoded = new byte[encodedLength];
        buffer.getBytes(0, encoded);

        return encoded;
    }

    private BooleanType mapBoolean(Boolean value) {
        if (value == null) {
            return BooleanType.NULL_VAL;
        }
        return value ? BooleanType.TRUE : BooleanType.FALSE;
    }
}

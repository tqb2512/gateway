package com.etrade.gateway.application.service;

import com.etrade.gateway.application.disruptor.MarketDataEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

/**
 * WebSocket handler that acts as a thin Disruptor producer.
 * Receives market data JSON from the WebSocket and publishes it
 * into the Disruptor ring buffer for downstream processing.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class MarketDataWebSocketHandler extends TextWebSocketHandler {

    private final Disruptor<MarketDataEvent> disruptor;
    private final ObjectMapper objectMapper;

    private WebSocketConnectionManager connectionManager;

    public void setConnectionManager(WebSocketConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        log.info("WebSocket connected: sessionId={}, uri={}", session.getId(), session.getUri());
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) {
        String payload = message.getPayload();
        log.debug("Received WebSocket message: {} bytes", payload.length());

        try {
            JsonNode root = objectMapper.readTree(payload);

            if (root.isArray()) {
                log.debug("Received array message with {} elements", root.size());
                for (JsonNode element : root) {
                    publishToDisruptor(element.toString());
                }
            } else {
                publishToDisruptor(payload);
            }
        } catch (Exception e) {
            log.error("Failed to process WebSocket message: {}", e.getMessage(), e);
        }
    }

    /**
     * Publish a single raw JSON quote string into the Disruptor ring buffer.
     */
    private void publishToDisruptor(String quoteJson) {
        RingBuffer<MarketDataEvent> ringBuffer = disruptor.getRingBuffer();
        long sequence = ringBuffer.next();
        try {
            MarketDataEvent event = ringBuffer.get(sequence);
            event.setRawJson(quoteJson);
        } finally {
            ringBuffer.publish(sequence);
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        log.warn("WebSocket disconnected: sessionId={}, status={}", session.getId(), status);
        if (connectionManager != null) {
            connectionManager.scheduleReconnect();
        }
    }

    @Override
    public void handleTransportError(WebSocketSession session, Throwable exception) {
        log.error("WebSocket transport error: {}", exception.getMessage(), exception);
        if (connectionManager != null) {
            connectionManager.scheduleReconnect();
        }
    }
}

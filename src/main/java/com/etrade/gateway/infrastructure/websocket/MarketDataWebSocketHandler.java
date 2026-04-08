package com.etrade.gateway.infrastructure.websocket;

import com.etrade.gateway.domain.service.PublishService;
import com.etrade.gateway.infrastructure.monitoring.ThroughputMonitor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

/**
 * WebSocket handler that acts as a thin Disruptor producer.
 * Receives market data JSON from the WebSocket and publishes it
 * via {@link PublishService} for downstream processing.
 */
@Slf4j
@Component
public class MarketDataWebSocketHandler extends TextWebSocketHandler {

    private final PublishService<Void, String> publishService;
    private final ObjectMapper objectMapper;
    private final ThroughputMonitor throughputMonitor;

    private WebSocketConnectionManager connectionManager;

    public MarketDataWebSocketHandler(
            @Qualifier("MarketDataDisruptor") PublishService<Void, String> publishService,
            ObjectMapper objectMapper,
            ThroughputMonitor throughputMonitor) {
        this.publishService = publishService;
        this.objectMapper = objectMapper;
        this.throughputMonitor = throughputMonitor;
    }

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

        throughputMonitor.increment("ws-messages-in");

        try {
            JsonNode root = objectMapper.readTree(payload);

            if (root.isArray()) {
                log.debug("Received array message with {} elements", root.size());
                for (JsonNode element : root) {
                    publishService.publish("", element.toString());
                }
                throughputMonitor.add("ws-quotes-in", root.size());
            } else {
                publishService.publish("", payload);
                throughputMonitor.increment("ws-quotes-in");
            }
        } catch (Exception e) {
            log.error("Failed to process WebSocket message: {}", e.getMessage(), e);
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

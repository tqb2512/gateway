package com.etrade.gateway.infrastructure.websocket;

import com.etrade.gateway.domain.service.PublishService;
import com.etrade.gateway.infrastructure.monitoring.ThroughputMonitor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NonNull;
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

    private final PublishService<Void, JsonNode> publishService;
    private final ObjectMapper objectMapper;
    private final ThroughputMonitor throughputMonitor;

    @Setter
    private WebSocketConnectionManager connectionManager;

    public MarketDataWebSocketHandler(
            @Qualifier("MarketDataDisruptor") PublishService<Void, JsonNode> publishService,
            ObjectMapper objectMapper,
            ThroughputMonitor throughputMonitor) {
        this.publishService = publishService;
        this.objectMapper = objectMapper;
        this.throughputMonitor = throughputMonitor;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        log.info("WebSocket connected: sessionId={}, uri={}", session.getId(), session.getUri());
    }

    @Override
    protected void handleTextMessage(@NonNull WebSocketSession session, TextMessage message) {
        String payload = message.getPayload();
        throughputMonitor.increment("ws-messages-in");

        try {
            // Parse once on the WS thread; forward the JsonNode down the
            // Disruptor so the event handler does not re-parse the JSON.
            JsonNode root = objectMapper.readTree(payload);

            if (root.isArray()) {
                int size = root.size();
                for (int i = 0; i < size; i++) {
                    publishService.publish("", root.get(i));
                }
                throughputMonitor.add("ws-quotes-in", size);
            } else {
                publishService.publish("", root);
                throughputMonitor.increment("ws-quotes-in");
            }
        } catch (Exception e) {
            log.error("Failed to process WebSocket message: {}", e.getMessage(), e);
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, @NonNull CloseStatus status) {
        log.warn("WebSocket disconnected: sessionId={}, status={}", session.getId(), status);
        if (connectionManager != null) {
            connectionManager.scheduleReconnect();
        }
    }

    @Override
    public void handleTransportError(@NonNull WebSocketSession session, @NonNull Throwable exception) {
        log.error("WebSocket transport error: {}", exception.getMessage(), exception);
        if (connectionManager != null) {
            connectionManager.scheduleReconnect();
        }
    }
}

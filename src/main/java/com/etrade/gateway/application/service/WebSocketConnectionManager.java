package com.etrade.gateway.application.service;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketHttpHeaders;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;

import java.net.URI;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Component
@RequiredArgsConstructor
public class WebSocketConnectionManager {

    private final StandardWebSocketClient webSocketClient;
    private final MarketDataWebSocketHandler webSocketHandler;

    @Value("${scb.websocket.uri}")
    private String websocketUri;

    @Value("${scb.client-id}")
    private String clientId;

    @Value("${scb.websocket.reconnect.initial-delay-ms:1000}")
    private long initialDelayMs;

    @Value("${scb.websocket.reconnect.max-delay-ms:30000}")
    private long maxDelayMs;

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final AtomicBoolean connected = new AtomicBoolean(false);
    private final AtomicInteger reconnectAttempt = new AtomicInteger(0);

    private volatile WebSocketSession session;

    @PostConstruct
    public void init() {
        webSocketHandler.setConnectionManager(this);
        connect();
    }

    public void connect() {
        String uri = websocketUri.replace("{clientId}", clientId);
        log.info("Connecting to WebSocket: {}", uri);

        try {
            session = webSocketClient
                    .execute(webSocketHandler, new WebSocketHttpHeaders(), URI.create(uri))
                    .get(10, TimeUnit.SECONDS);

            connected.set(true);
            reconnectAttempt.set(0);
            log.info("WebSocket connection established: sessionId={}", session.getId());
        } catch (Exception e) {
            log.error("Failed to connect to WebSocket: {}", e.getMessage(), e);
            connected.set(false);
            scheduleReconnect();
        }
    }

    public void scheduleReconnect() {
        if (!connected.compareAndSet(true, false) && reconnectAttempt.get() > 0) {
            // Already reconnecting
            return;
        }

        int attempt = reconnectAttempt.incrementAndGet();
        long delay = Math.min(initialDelayMs * (1L << (attempt - 1)), maxDelayMs);

        log.info("Scheduling reconnect attempt {} in {}ms", attempt, delay);
        scheduler.schedule(this::connect, delay, TimeUnit.MILLISECONDS);
    }

    public boolean isConnected() {
        return connected.get() && session != null && session.isOpen();
    }

    public WebSocketSession getSession() {
        return session;
    }
}

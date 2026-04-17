package com.etrade.gateway.application.service;

import com.etrade.gateway.infrastructure.monitoring.ThroughputMonitor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Publishes a batch of encoded byte[] records to Kafka.
 * Sends all records via {@link KafkaTemplate#send}, then calls
 * {@link KafkaTemplate#flush()}
 * to push them in a single network round-trip.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaBatchPublishService {

    private final KafkaTemplate<String, byte[]> kafkaTemplate;
    private final ThroughputMonitor throughputMonitor;

    public void publishBatch(String topic, List<byte[]> batch) {
        if (batch == null || batch.isEmpty()) {
            return;
        }

        final int size = batch.size();
        for (int i = 0; i < size; i++) {
            final byte[] record = batch.get(i);
            kafkaTemplate.send(topic, record).whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("Kafka send failed on topic [{}]: {}", topic, ex.getMessage());
                }
            });
        }

        throughputMonitor.add("kafka-records-out", size);
        throughputMonitor.increment("kafka-batches-out");

        if (log.isDebugEnabled()) {
            log.debug("Dispatched batch of {} records to topic [{}]", size, topic);
        }
    }
}

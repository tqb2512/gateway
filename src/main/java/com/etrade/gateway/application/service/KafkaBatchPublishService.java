package com.etrade.gateway.application.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Publishes a batch of encoded byte[] records to Kafka.
 * Sends all records via {@link KafkaTemplate#send}, then calls {@link KafkaTemplate#flush()}
 * to push them in a single network round-trip.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaBatchPublishService {

    private final KafkaTemplate<String, byte[]> kafkaTemplate;

    /**
     * Publish a batch of encoded records to the specified Kafka topic.
     *
     * @param topic the Kafka topic
     * @param batch list of encoded byte arrays
     */
    public void publishBatch(String topic, List<byte[]> batch) {
        if (batch == null || batch.isEmpty()) {
            return;
        }

        long totalBytes = 0;

        for (byte[] record : batch) {
            kafkaTemplate.send(topic, record);
            totalBytes += record.length;
        }

        // Flush all buffered records to the broker in one go
        kafkaTemplate.flush();

        log.info("Published batch of {} records ({} bytes) to topic [{}]",
                batch.size(), totalBytes, topic);
    }
}

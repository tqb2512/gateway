package com.etrade.gateway.application.service;

import com.etrade.gateway.domain.service.PublishService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaPublishService implements PublishService<CompletableFuture<SendResult<String, byte[]>>, byte[]> {

    private final KafkaTemplate<String, byte[]> kafkaTemplate;

    @Override
    public CompletableFuture<SendResult<String, byte[]>> publish(String topic, byte[] data) {
        log.debug("Publishing {} bytes to topic [{}]", data.length, topic);

        return kafkaTemplate.send(topic, data)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish to topic [{}]: {}", topic, ex.getMessage(), ex);
                    } else {
                        log.debug("Published to topic [{}] partition [{}] offset [{}]",
                                topic,
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    }
                });
    }
}

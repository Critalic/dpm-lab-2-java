package com.kpi.producer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kpi.producer.configuration.KafkaProperties;
import com.kpi.producer.dto.KafkaMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;

@Slf4j
@ConditionalOnProperty(name = "producer.single-partition.enabled", havingValue = "true", matchIfMissing = true)
@Service
public class SinglePartitionService extends KafkaProducerService {
    private final ObjectMapper objectMapper;
    private final KafkaProperties kafkaProperties;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public SinglePartitionService(ObjectMapper objectMapper, KafkaProperties kafkaProperties, KafkaTemplate<String, String> kafkaTemplate) {
        this.objectMapper = objectMapper;
        this.kafkaProperties = kafkaProperties;
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(int messageCount) throws JsonProcessingException {
        Instant start = Instant.now();
        for (int i = 0; i < messageCount; i++) {
            KafkaMessage kafkaMessage = generateMessage();
            final String payload = objectMapper.writeValueAsString(kafkaMessage);

            ProducerRecord<String, String> record = new ProducerRecord<>(kafkaProperties.topic(), payload);
            CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(record);
            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    log.info("Sent message to partition=[{}]",
                            result.getRecordMetadata().partition());
                } else {
                    log.error("Unable to send message=[{}] due to: {}", payload, ex.getMessage());
                }
            });
        }
        Instant end = Instant.now();
        log.info("Finished sending {} messages, time {}", messageCount, Duration.between(start, end).toMillis());
        log.info("Finished sending {} messages", messageCount);
    }
}

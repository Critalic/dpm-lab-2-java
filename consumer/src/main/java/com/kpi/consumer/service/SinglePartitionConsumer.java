package com.kpi.consumer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kpi.consumer.dto.CityDataDto;
import com.kpi.consumer.dto.KafkaMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class SinglePartitionConsumer {
    private final ObjectMapper objectMapper;

    public SinglePartitionConsumer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "${consumer.kafka.topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void consume(
            @Payload String messageString,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.RECEIVED_KEY) String key) throws JsonProcessingException {

        var message = objectMapper.readValue(messageString, KafkaMessage.class);
        var cityData = message.payload().cityData();
        log.info("Consumer received: city=[{}], state=[{}], partition=[{}], key=[{}]",
                cityData.city(), cityData.state(), partition, key);
    }
}

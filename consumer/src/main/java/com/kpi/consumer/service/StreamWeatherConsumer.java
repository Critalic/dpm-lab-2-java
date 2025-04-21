package com.kpi.consumer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kpi.consumer.dto.KafkaMessageForecast;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

//Lab 3
@Slf4j
@Component
@RequiredArgsConstructor
public class StreamWeatherConsumer {
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = "${consumer.stream.kafka.topic12}",
            groupId = "${consumer.stream.kafka.group-id}",
            batch = "false",
            containerFactory = "batchFactory"
    )
    public void consume12(
            @Payload String messageString) throws JsonProcessingException {
        var message = objectMapper.readValue(messageString, KafkaMessageForecast.class);
        var forecast = message.payload().forecastDto();
        log.info("Message received: {}", forecast);
    }

    @KafkaListener(
            topics = "${consumer.stream.kafka.topic24}",
            groupId = "${consumer.stream.kafka.group-id}",
            batch = "false",
            containerFactory = "batchFactory"
    )
    public void consume24(
            @Payload String messageString) throws JsonProcessingException {
        var message = objectMapper.readValue(messageString, KafkaMessageForecast.class);
        var forecast = message.payload().forecastDto();
        log.info("Message received: {}", forecast);
    }

    @KafkaListener(
            topics = "${consumer.stream.kafka.topic36}",
            groupId = "${consumer.stream.kafka.group-id}",
            batch = "false",
            containerFactory = "batchFactory"
    )
    public void consume36(
            @Payload String messageString) throws JsonProcessingException {
        var message = objectMapper.readValue(messageString, KafkaMessageForecast.class);
        var forecast = message.payload().forecastDto();
        log.info("Message received: {}", forecast);
    }

    @KafkaListener(
            topics = "${consumer.stream.kafka.topic48}",
            groupId = "${consumer.stream.kafka.group-id}",
            batch = "false",
            containerFactory = "batchFactory"
    )
    public void consume48(
            @Payload String messageString) throws JsonProcessingException {
        var message = objectMapper.readValue(messageString, KafkaMessageForecast.class);
        var forecast = message.payload().forecastDto();
        log.info("Message received: {}", forecast);
    }
}

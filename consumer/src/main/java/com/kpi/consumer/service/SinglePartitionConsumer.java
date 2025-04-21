package com.kpi.consumer.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

//Lab 2
@Slf4j
@Service
public class SinglePartitionConsumer {
    private final ObjectMapper objectMapper;

    public SinglePartitionConsumer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

//    @KafkaListener(
//            topics = "${consumer.kafka.topic}",
//            groupId = "${spring.kafka.consumer.group-id}",
//            batch = "false",
//            containerFactory = "batchFactory"
//            )
//    public void consume(
//            @Payload String messageString) throws InterruptedException {
//
////        var message = objectMapper.readValue(messageString, KafkaMessage.class);
////        var cityData = message.payload().cityData();
//
////        Thread.sleep(10);
//        log.info("Received record: {}", messageString);
//    }

//    @KafkaListener(
//            topics = "${consumer.kafka.topic}",
//            groupId = "${spring.kafka.consumer.group-id}",
//            batch = "true",
//            containerFactory = "batchFactory"
//    )
//    public void consume(
//            ConsumerRecords<String, String> records) {
//
//        records.forEach(record -> {
//            log.info("Received record: {}", record.value());
//        });
//    }
}

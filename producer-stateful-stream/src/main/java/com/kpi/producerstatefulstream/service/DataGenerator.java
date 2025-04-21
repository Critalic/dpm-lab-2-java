package com.kpi.producerstatefulstream.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kpi.producerstatefulstream.dto.GenData;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

@Slf4j
@Service
@RequiredArgsConstructor
public class DataGenerator {
    @Value("${gen-data.stream.kafka.topic}")
    private String topic;
    private final KafkaTemplate<String, String> kafkaTemplate;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Random random = new Random();
    private final List<String> cities = Arrays.asList(
            "New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
            "Philadelphia", "San Antonio", "San Diego", "Dallas", "Austin");

    private final List<String> states = Arrays.asList(
            "NY", "CA", "IL", "TX", "AZ", "PA", "TX", "CA", "TX", "TX");

    public void send(int messageCount) throws JsonProcessingException {
        for (int i = 0; i < messageCount; i++) {
            GenData message = generateData();
            String json = objectMapper.writeValueAsString(message);

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, message.city(), json);
            kafkaTemplate.send(record);
        }
    }

    private GenData generateData() {
        return new GenData(
                cities.get(random.nextInt(cities.size())),
                states.get(random.nextInt(states.size())),
                random.nextInt(10, 100),
                random.nextInt(2)
        );
    }
}

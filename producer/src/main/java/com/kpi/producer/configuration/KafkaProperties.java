package com.kpi.producer.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(value = "producer.kafka")
public record KafkaProperties(String topic, Integer partitionCount) {
}

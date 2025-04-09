package com.kpi.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kpi.producer.service.KafkaProducerService;
import com.kpi.producer.service.SinglePartitionService;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

@SpringBootApplication
@EnableScheduling
@ConfigurationPropertiesScan
public class ProducerApplication {
    private final KafkaProducerService singlePartitionService;

    public ProducerApplication(KafkaProducerService singlePartitionService) {
        this.singlePartitionService = singlePartitionService;
    }

    public static void main(String[] args) {
        SpringApplication.run(ProducerApplication.class, args);
    }

    @Scheduled(initialDelay = 1000, fixedDelay=Long.MAX_VALUE)
    private void sendMessage() {
        try {
            singlePartitionService.send(300000);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

}

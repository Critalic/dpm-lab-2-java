package com.kpi.producerstatefulstream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kpi.producerstatefulstream.service.DataGenerator;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

@RequiredArgsConstructor
@SpringBootApplication
@EnableScheduling
public class ProducerStatefulStreamApplication {
	private final DataGenerator dataGenerator;

	public static void main(String[] args) {
		SpringApplication.run(ProducerStatefulStreamApplication.class, args);
	}

	@Scheduled(initialDelay = 1000, fixedDelay=Long.MAX_VALUE)
	private void sendMessage() {
//		try {
//			dataGenerator.send(3000);
//		} catch (JsonProcessingException e) {
//			throw new RuntimeException(e);
//		}
	}

}

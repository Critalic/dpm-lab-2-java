package com.kpi.producer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kpi.producer.dto.CityDataDto;
import com.kpi.producer.dto.KafkaMessage;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

public abstract class KafkaProducerService {
    private final Random random = new Random();
    // Sample data for random generation
    private final List<String> cities = Arrays.asList(
            "New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
            "Philadelphia", "San Antonio", "San Diego", "Dallas", "Austin");

    private final List<String> states = Arrays.asList(
            "NY", "CA", "IL", "TX", "AZ", "PA", "TX", "CA", "TX", "TX");

    private final List<String> koppenTypes = Arrays.asList(
            "Dfa", "Csa", "Dfa", "Cfa", "BWh", "Dfa", "Cfa", "Csa", "Cfa", "Cfa");

    public abstract void send(int messageCount) throws JsonProcessingException;

    public KafkaMessage generateMessage() {
        var cityData = generateRandomCityData();
        return new KafkaMessage(new KafkaMessage.Payload(cityData));
    }

    private CityDataDto generateRandomCityData() {
        int index = random.nextInt(cities.size());
        String city = cities.get(index);
        String state = states.get(index);
        String koppen = koppenTypes.get(index);

        return new CityDataDto(
                city,
                state,
                -180.0 + random.nextDouble() * 360.0,
                -90.0 + random.nextDouble() * 180.0,
                koppen,
                random.nextDouble() * 5000.0,
                random.nextDouble() * 1000.0,
                random.nextDouble() * 30.0,
                random.nextDouble() * 200.0,
                random.nextDouble() * 500.0,
                random.nextDouble() * 2000.0
        );
    }
}

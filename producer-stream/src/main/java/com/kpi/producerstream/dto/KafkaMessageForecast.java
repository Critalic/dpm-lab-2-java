package com.kpi.producerstream.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record KafkaMessageForecast(Payload payload) {
    public record Payload(@JsonProperty("after") ForecastDto forecastDto) {

    }
}
package com.kpi.producerstatefulstream.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record KafkaMessageForecast(Payload payload) {
    public ForecastDto getForecastDto() {
        return payload().forecastDto();
    }

    public record Payload(@JsonProperty("after") ForecastDto forecastDto) {

    }
}
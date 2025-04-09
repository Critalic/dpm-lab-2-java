package com.kpi.consumer.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record KafkaMessageCity(Payload payload) {
    public record Payload(@JsonProperty("after") CityDto cityData) {

    }
}
package com.kpi.consumer.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record KafkaMessage(Payload payload) {
    public record Payload(@JsonProperty("after") CityDataDto cityData) {

    }
}
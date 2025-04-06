package com.kpi.producer.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public record KafkaMessage(Payload payload) {
    public record Payload(@JsonProperty("after") CityDataDto cityData) {

    }
}
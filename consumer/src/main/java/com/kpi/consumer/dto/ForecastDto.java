package com.kpi.consumer.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.io.IOException;
import java.time.LocalDate;

public record ForecastDto(
        @JsonDeserialize(using = DateArrayDeserializer.class)
        LocalDate date,
        String city,
        String state,
        @JsonProperty("high_or_low")
        String highOrLow,
        @JsonProperty("forecast_hours_before")
        int forecastHoursBefore,
        @JsonProperty("observed_temp")
        Integer observedTemp,
        @JsonProperty("forecast_temp")
        Integer forecastTemp,
        @JsonProperty("observed_precip")
        Float observedPrecip,
        @JsonProperty("forecast_outlook")
        String forecastOutlook,
        @JsonProperty("possible_error")
        String possibleError
) {
        public static class DateArrayDeserializer extends JsonDeserializer<LocalDate> {
                @Override
                public LocalDate deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
                        // Convert JSON array into an array of integers
                        JsonNode node = p.getCodec().readTree(p);
                        int year = node.get(0).asInt();
                        int month = node.get(1).asInt();
                        int day = node.get(2).asInt();

                        return LocalDate.of(year, month, day);
                }
        }
}

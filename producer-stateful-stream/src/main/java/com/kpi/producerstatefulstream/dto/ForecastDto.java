package com.kpi.producerstatefulstream.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.io.IOException;
import java.time.LocalDate;

public record ForecastDto(
        @JsonDeserialize(using = DaysSinceEpochDeserializer.class)
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
    public static class DaysSinceEpochDeserializer extends JsonDeserializer<LocalDate> {
        @Override
        public LocalDate deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            long daysSinceEpoch = p.getLongValue();
            return LocalDate.ofEpochDay(daysSinceEpoch);
        }
    }
}

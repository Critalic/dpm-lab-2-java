package com.kpi.consumer.dto;

public record CityDataDto (
        String city,
        String state,
        Double lon,
        Double lat,
        String koppen,
        Double elevation,
        Double distance_to_coast,
        Double wind,
        Double elevation_change_four,
        Double elevation_change_eight,
        Double avg_annual_precip
) {}

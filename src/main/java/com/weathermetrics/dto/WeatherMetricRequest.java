package com.weathermetrics.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;

import java.util.Map;

public record WeatherMetricRequest(
        @NotBlank(message = "sensorId is required")
        String sensorId,

        String timestamp,

        @NotEmpty(message = "metrics must contain at least one entry")
        Map<String, Double> metrics
) {
}

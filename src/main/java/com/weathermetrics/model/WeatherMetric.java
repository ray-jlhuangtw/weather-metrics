package com.weathermetrics.model;

import java.util.Collections;
import java.util.Map;

public record WeatherMetric(String sensorId, String timestamp, Map<String, Double> metrics) {

    public WeatherMetric {
        metrics = metrics != null ? Map.copyOf(metrics) : Collections.emptyMap();
    }
}

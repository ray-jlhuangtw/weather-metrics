package com.weathermetrics.dto;

import java.util.List;
import java.util.Map;

public record WeatherMetricQueryResponse(List<String> queriedSensorIds, List<String> statistics, String startDate, String endDate, List<SensorResult> sensorResults) {

    public record SensorResult(String sensorId, Map<String, Map<String, Double>> metrics) {}
}

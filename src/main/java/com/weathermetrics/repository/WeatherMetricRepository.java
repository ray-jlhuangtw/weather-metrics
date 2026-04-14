package com.weathermetrics.repository;

import com.weathermetrics.model.WeatherMetric;

import java.util.List;
import java.util.Optional;
import java.util.Set;

public interface WeatherMetricRepository {
    void save(WeatherMetric metric);

    List<WeatherMetric> queryBySensorAndTimeRange(String sensorId, String startTime, String endTime);

    Optional<WeatherMetric> getLatestBySensor(String sensorId);

    Set<String> getAllSensorIds();
}

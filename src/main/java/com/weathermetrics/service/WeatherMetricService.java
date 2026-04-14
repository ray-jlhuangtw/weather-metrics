package com.weathermetrics.service;

import com.weathermetrics.dto.MetricQuery;
import com.weathermetrics.model.Statistic;
import com.weathermetrics.model.WeatherMetric;
import com.weathermetrics.repository.WeatherMetricRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.*;

@Service
public class WeatherMetricService {

    private static final Logger LOGGER = LoggerFactory.getLogger(WeatherMetricService.class);

    private final WeatherMetricRepository weatherMetricRepository;

    public WeatherMetricService(WeatherMetricRepository weatherMetricRepository) {
        this.weatherMetricRepository = weatherMetricRepository;
    }

    public WeatherMetric saveMetric(String sensorId, String timestamp, Map<String, Double> metrics) {
        LOGGER.info("Saving metric for sensor={}", sensorId);

        if (timestamp == null || timestamp.isBlank()) {
            timestamp = Instant.now().toString();
        } else {
            try {
                Instant.parse(timestamp);
            } catch (DateTimeParseException e) {
                throw new IllegalArgumentException("Invalid timestamp format.");
            }
        }

        for (Map.Entry<String, Double> entry : metrics.entrySet()) {
            String key = entry.getKey();
            if (key == null || key.isBlank()) {
                throw new IllegalArgumentException("Metric names must not be blank");
            }
        }

        WeatherMetric metric = new WeatherMetric(sensorId, timestamp, metrics);
        weatherMetricRepository.save(metric);
        return metric;
    }

    public Map<String, Map<String, Map<String, Double>>> queryMetrics(MetricQuery query) {
        LOGGER.info("Querying metrics: sensors={}, statistics={}, dateRange=[{}]", query.sensorIds(), query.statistics(), query.timeRange());

        List<String> sensorIds = query.sensorIds();
        if (sensorIds == null || sensorIds.isEmpty()) {
            Set<String> allIds = weatherMetricRepository.getAllSensorIds();
            if (allIds.isEmpty()) {
                return Collections.emptyMap();
            }
            sensorIds = new ArrayList<>(allIds);
        }

        Map<String, Map<String, Map<String, Double>>> results = new LinkedHashMap<>();

        for (String sensorId : sensorIds) {
            List<WeatherMetric> weatherMetrics;
            if (query.hasDateRange()) {
                weatherMetrics = weatherMetricRepository.queryBySensorAndTimeRange(
                        sensorId,
                        query.timeRange().startAsString(),
                        query.timeRange().endAsString());
            } else {
                weatherMetrics = weatherMetricRepository.getLatestBySensor(sensorId)
                        .map(List::of)
                        .orElse(Collections.emptyList());
            }

            if (weatherMetrics.isEmpty()) {
                continue;
            }

            Map<String, Map<String, Double>> aggregated = aggregate(weatherMetrics, query.metricNames(), query.statistics());
            if (!aggregated.isEmpty()) {
                results.put(sensorId, aggregated);
            }
        }

        return results;
    }

    Map<String, Map<String, Double>> aggregate(List<WeatherMetric> weatherMetrics, List<String> metricNames, List<Statistic> statistics) {
        List<Statistic> effectiveStatistics = (statistics == null || statistics.isEmpty()) ? List.of(Statistic.values()) : statistics;
        Set<String> targetMetrics;
        if (metricNames == null || metricNames.isEmpty()) {
            targetMetrics = new LinkedHashSet<>();
            for (WeatherMetric reading : weatherMetrics) {
                targetMetrics.addAll(reading.metrics().keySet());
            }
        } else {
            targetMetrics = new LinkedHashSet<>(metricNames);
        }

        Map<String, Map<String, Double>> result = new LinkedHashMap<>();

        for (String metricName : targetMetrics) {
            List<Double> values = new ArrayList<>();
            for (WeatherMetric reading : weatherMetrics) {
                Double val = reading.metrics().get(metricName);
                if (val != null) {
                    values.add(val);
                }
            }

            if (values.isEmpty()) {
                continue;
            }

            Map<String, Double> statMap = new LinkedHashMap<>();
            for (Statistic stat : effectiveStatistics) {
                statMap.put(stat.name().toLowerCase(), stat.compute(values));
            }
            result.put(metricName, statMap);
        }

        return result;
    }
}

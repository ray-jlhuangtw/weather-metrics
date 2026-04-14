package com.weathermetrics.controller;

import com.weathermetrics.dto.MetricQuery;
import com.weathermetrics.dto.TimeRange;
import com.weathermetrics.dto.WeatherMetricQueryResponse;
import com.weathermetrics.dto.WeatherMetricQueryResponse.SensorResult;
import com.weathermetrics.dto.WeatherMetricRequest;
import com.weathermetrics.model.Statistic;
import com.weathermetrics.model.WeatherMetric;
import com.weathermetrics.service.WeatherMetricService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/metrics")
public class WeatherMetricController {

    private final WeatherMetricService weatherMetricService;

    public WeatherMetricController(WeatherMetricService weatherMetricService) {
        this.weatherMetricService = weatherMetricService;
    }

    @PostMapping
    public ResponseEntity<WeatherMetric> createMetric(@Valid @RequestBody WeatherMetricRequest request) {
        WeatherMetric saved = weatherMetricService.saveMetric(request.sensorId(), request.timestamp(), request.metrics());
        return ResponseEntity.status(HttpStatus.CREATED).body(saved);
    }

    @GetMapping
    public ResponseEntity<WeatherMetricQueryResponse> queryMetrics(
            @RequestParam(required = false) List<String> sensorIds,
            @RequestParam(required = false) List<String> metrics,
            @RequestParam(required = false) List<String> statistics,
            @RequestParam(required = false) String startDate,
            @RequestParam(required = false) String endDate) {

        List<Statistic> stats = Statistic.fromStrings(statistics);
        List<Statistic> effectiveStats = stats.isEmpty() ? List.of(Statistic.values()) : stats;
        TimeRange timeRange = parseTimeRange(startDate, endDate);

        MetricQuery query = new MetricQuery(sensorIds, metrics, stats, timeRange);
        Map<String, Map<String, Map<String, Double>>> results = weatherMetricService.queryMetrics(query);

        List<SensorResult> sensorResults = results.entrySet().stream()
                .map(e -> new SensorResult(e.getKey(), e.getValue()))
                .toList();

        List<String> queriedIds = sensorIds != null ? sensorIds : results.keySet().stream().toList();
        List<String> statisticNames = effectiveStats.stream().map(s -> s.name().toLowerCase()).toList();
        WeatherMetricQueryResponse response = new WeatherMetricQueryResponse(
                queriedIds, statisticNames, startDate, endDate, sensorResults);

        return ResponseEntity.ok(response);
    }

    private TimeRange parseTimeRange(String startDate, String endDate) {
        if (startDate == null && endDate == null) {
            return null;
        }
        if (startDate == null || endDate == null) {
            throw new IllegalArgumentException("Both startDate and endDate must be provided together");
        }
        return TimeRange.of(startDate, endDate);
    }
}

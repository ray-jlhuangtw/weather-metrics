package com.weathermetrics.service;

import com.weathermetrics.dto.MetricQuery;
import com.weathermetrics.dto.TimeRange;
import com.weathermetrics.model.Statistic;
import com.weathermetrics.model.WeatherMetric;
import com.weathermetrics.repository.WeatherMetricRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class WeatherMetricServiceTest {

    @Mock
    private WeatherMetricRepository repository;

    private WeatherMetricService service;

    @BeforeEach
    void setUp() {
        service = new WeatherMetricService(repository);
    }

    @Test
    void saveMetric_withTimestamp_savesAsProvided() {
        Map<String, Double> metrics = Map.of("temperature", 22.5);
        WeatherMetric result = service.saveMetric("sensor-1", "2026-04-11T14:00:00Z", metrics);

        assertThat(result.sensorId()).isEqualTo("sensor-1");
        assertThat(result.timestamp()).isEqualTo("2026-04-11T14:00:00Z");
        assertThat(result.metrics()).isEqualTo(metrics);
        verify(repository).save(any(WeatherMetric.class));
    }

    @Test
    void saveMetric_withoutTimestamp_generatesTimestamp() {
        Map<String, Double> metrics = Map.of("temperature", 22.5);
        WeatherMetric result = service.saveMetric("sensor-1", null, metrics);

        assertThat(result.timestamp()).isNotNull();
        assertThat(result.timestamp()).isNotBlank();
        verify(repository).save(any(WeatherMetric.class));
    }

    @Test
    void saveMetric_withInvalidTimestamp_throwsIllegalArgument() {
        assertThatThrownBy(() -> service.saveMetric("sensor-1", "not-a-date", Map.of("temp", 1.0)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid timestamp format");
    }

    @Test
    void queryMetrics_withDateRange_queriesRepository() {
        when(repository.queryBySensorAndTimeRange("sensor-1", "2026-04-01T00:00:00Z", "2026-04-11T00:00:00Z"))
                .thenReturn(List.of(new WeatherMetric("sensor-1", "2026-04-05T00:00:00Z", Map.of("temperature", 22.0))));

        MetricQuery query = new MetricQuery(List.of("sensor-1"), List.of("temperature"), List.of(Statistic.AVERAGE), TimeRange.of("2026-04-01T00:00:00Z", "2026-04-11T00:00:00Z"));
        Map<String, Map<String, Map<String, Double>>> result = service.queryMetrics(query);

        assertThat(result).containsKey("sensor-1");
        assertThat(result.get("sensor-1").get("temperature")).containsEntry("average", 22.0);
    }

    @Test
    void queryMetrics_withoutDateRange_getsLatest() {
        when(repository.getLatestBySensor("sensor-1")).thenReturn(Optional.of(new WeatherMetric("sensor-1", "2026-04-11T00:00:00Z", Map.of("temperature", 25.0))));

        MetricQuery query = new MetricQuery(List.of("sensor-1"), null, List.of(Statistic.AVERAGE), null);
        Map<String, Map<String, Map<String, Double>>> result = service.queryMetrics(query);

        assertThat(result).containsKey("sensor-1");
        assertThat(result.get("sensor-1").get("temperature")).containsEntry("average", 25.0);
    }

    @Test
    void queryMetrics_allSensors_scansForIds() {
        when(repository.getAllSensorIds()).thenReturn(Set.of("sensor-1"));
        when(repository.getLatestBySensor("sensor-1")).thenReturn(Optional.of(new WeatherMetric("sensor-1", "2026-04-11T00:00:00Z", Map.of("temperature", 25.0))));

        MetricQuery query = new MetricQuery(null, null, List.of(Statistic.AVERAGE), null);
        Map<String, Map<String, Map<String, Double>>> result = service.queryMetrics(query);

        assertThat(result).containsKey("sensor-1");
        verify(repository).getAllSensorIds();
    }

    @Test
    void queryMetrics_sensorWithNoReadings_isExcludedFromResult() {
        when(repository.getLatestBySensor("sensor-1")).thenReturn(Optional.empty());

        MetricQuery query = new MetricQuery(List.of("sensor-1"), null, List.of(Statistic.AVERAGE), null);
        Map<String, Map<String, Map<String, Double>>> result = service.queryMetrics(query);

        assertThat(result).doesNotContainKey("sensor-1");
    }

    @Test
    void queryMetrics_multipleStatistics_returnsAllStatistics() {
        when(repository.getLatestBySensor("sensor-1")).thenReturn(Optional.of(
                new WeatherMetric("sensor-1", "2026-04-11T00:00:00Z", Map.of("temperature", 20.0))));

        MetricQuery query = new MetricQuery(List.of("sensor-1"), null, List.of(Statistic.MIN, Statistic.MAX), null);
        Map<String, Map<String, Map<String, Double>>> result = service.queryMetrics(query);

        assertThat(result).containsKey("sensor-1");
        Map<String, Double> tempStats = result.get("sensor-1").get("temperature");
        assertThat(tempStats).containsEntry("min", 20.0);
        assertThat(tempStats).containsEntry("max", 20.0);
    }

    @Test
    void aggregate_singleStatistic_matchesSingleAggregate() {
        List<WeatherMetric> readings = List.of(
                new WeatherMetric("s", "t1", Map.of("temperature", 10.0)),
                new WeatherMetric("s", "t2", Map.of("temperature", 30.0)));

        Map<String, Map<String, Double>> result =
                service.aggregate(readings, null, List.of(Statistic.AVERAGE));

        assertThat(result.get("temperature")).containsEntry("average", 20.0);
    }

    @Test
    void aggregate_multipleStatistics_returnsAllValues() {
        List<WeatherMetric> readings = List.of(
                new WeatherMetric("s", "t1", Map.of("temperature", 10.0)),
                new WeatherMetric("s", "t2", Map.of("temperature", 30.0)));

        Map<String, Map<String, Double>> result =
                service.aggregate(readings, null, List.of(Statistic.MIN, Statistic.MAX, Statistic.AVERAGE));

        Map<String, Double> tempStats = result.get("temperature");
        assertThat(tempStats).containsEntry("min", 10.0);
        assertThat(tempStats).containsEntry("max", 30.0);
        assertThat(tempStats).containsEntry("average", 20.0);
    }

    @Test
    void aggregate_filteredMetricNames_onlyIncludesRequested() {
        List<WeatherMetric> readings = List.of(
                new WeatherMetric("s", "t1", Map.of("temperature", 20.0, "humidity", 60.0)));

        Map<String, Map<String, Double>> result =
                service.aggregate(readings, List.of("temperature"), List.of(Statistic.AVERAGE));

        assertThat(result).containsKey("temperature");
        assertThat(result).doesNotContainKey("humidity");
    }

    @Test
    void aggregate_emptyStatistics_defaultsToAll() {
        List<WeatherMetric> readings = List.of(
                new WeatherMetric("s", "t1", Map.of("temperature", 10.0)),
                new WeatherMetric("s", "t2", Map.of("temperature", 30.0)));

        Map<String, Map<String, Double>> result = service.aggregate(readings, null, Collections.emptyList());

        Map<String, Double> tempStats = result.get("temperature");
        assertThat(tempStats).containsKey("min");
        assertThat(tempStats).containsKey("max");
        assertThat(tempStats).containsKey("sum");
        assertThat(tempStats).containsKey("average");
    }

    @Test
    void queryMetrics_noStatistics_defaultsToAll() {
        when(repository.getLatestBySensor("sensor-1")).thenReturn(Optional.of(
                new WeatherMetric("sensor-1", "2026-04-11T00:00:00Z", Map.of("temperature", 20.0))));

        MetricQuery query = new MetricQuery(List.of("sensor-1"), null, Collections.emptyList(), null);
        Map<String, Map<String, Map<String, Double>>> result = service.queryMetrics(query);

        Map<String, Double> tempStats = result.get("sensor-1").get("temperature");
        assertThat(tempStats).containsKey("min");
        assertThat(tempStats).containsKey("max");
        assertThat(tempStats).containsKey("sum");
        assertThat(tempStats).containsKey("average");
    }
}

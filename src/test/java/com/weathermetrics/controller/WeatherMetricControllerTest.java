package com.weathermetrics.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.weathermetrics.dto.MetricQuery;
import com.weathermetrics.dto.WeatherMetricRequest;
import com.weathermetrics.model.WeatherMetric;
import com.weathermetrics.service.WeatherMetricService;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;

import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(WeatherMetricController.class)
class WeatherMetricControllerTest {

    private static final String SENSOR_ID = "sensor-1";

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockitoBean
    private WeatherMetricService weatherMetricService;

    @Test
    void postMetric_valid_returns201() throws Exception {
        WeatherMetricRequest request = new WeatherMetricRequest(SENSOR_ID, "2026-04-11T14:00:00Z", Map.of("temperature", 22.5));

        when(weatherMetricService.saveMetric(anyString(), anyString(), anyMap()))
                .thenReturn(new WeatherMetric("sensor-1", "2026-04-11T14:00:00Z", Map.of("temperature", 22.5)));

        performPost(request)
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.sensorId").value("sensor-1"))
                .andExpect(jsonPath("$.metrics.temperature").value(22.5));
    }

    @Test
    void postMetric_missingSensorId_returns400() throws Exception {
        WeatherMetricRequest request = new WeatherMetricRequest(null, null, Map.of("temperature", 22.5));

        performPost(request)
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").exists());
    }

    @Test
    void postMetric_emptyMetrics_returns400() throws Exception {
        WeatherMetricRequest request = new WeatherMetricRequest("sensor-1", null, Map.of());

        performPost(request)
                .andExpect(status().isBadRequest());
    }

    @Test
    void postMetric_invalidTimestamp_returns400() throws Exception {
        WeatherMetricRequest request = new WeatherMetricRequest("sensor-1", "not-a-date", Map.of("temperature", 22.5));

        when(weatherMetricService.saveMetric(anyString(), eq("not-a-date"), anyMap())).thenThrow(new IllegalArgumentException("Invalid timestamp format."));

        performPost(request)
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value("Invalid timestamp format."));
    }

    private ResultActions performPost(WeatherMetricRequest request) throws Exception {
        return mockMvc.perform(post("/api/v1/metrics").contentType(MediaType.APPLICATION_JSON).content(objectMapper.writeValueAsString(request)));
    }

    @Test
    void queryMetrics_validRequest_returns200() throws Exception {
        when(weatherMetricService.queryMetrics(any(MetricQuery.class))).thenReturn(
                Map.of("sensor-1", Map.of("temperature", Map.of("average", 25.0))));

        mockMvc.perform(get("/api/v1/metrics")
                        .param("sensorIds", "sensor-1")
                        .param("metrics", "temperature")
                        .param("statistics", "average")
                        .param("startDate", "2026-04-01T00:00:00Z")
                        .param("endDate", "2026-04-11T00:00:00Z"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.statistics[0]").value("average"))
                .andExpect(jsonPath("$.sensorResults[0].sensorId").value("sensor-1"))
                .andExpect(jsonPath("$.sensorResults[0].metrics.temperature.average").value(25.0));
    }

    @Test
    void queryMetrics_multipleSensorIdsAndMetrics_returns200() throws Exception {
        when(weatherMetricService.queryMetrics(any(MetricQuery.class))).thenReturn(Map.of(
                "sensor-1", Map.of("temperature", Map.of("min", 28.0), "humidity", Map.of("min", 40.0)),
                "sensor-2", Map.of("temperature", Map.of("min", 21.0), "humidity", Map.of("min", 44.0))));

        mockMvc.perform(get("/api/v1/metrics")
                        .param("sensorIds", "sensor-1", "sensor-2")
                        .param("metrics", "temperature", "humidity")
                        .param("statistics", "min")
                        .param("startDate", "2026-04-01T00:00:00Z")
                        .param("endDate", "2026-04-11T00:00:00Z"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.statistics[0]").value("min"))
                .andExpect(jsonPath("$.sensorResults.length()").value(2));
    }

    @Test
    void queryMetrics_noStatistics_defaultsToAllAndReturns200() throws Exception {
        when(weatherMetricService.queryMetrics(any(MetricQuery.class))).thenReturn(
                Map.of("sensor-1", Map.of("temperature", Map.of("min", 10.0, "max", 30.0, "sum", 60.0, "average", 20.0))));

        mockMvc.perform(get("/api/v1/metrics")
                        .param("sensorIds", "sensor-1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.statistics", containsInAnyOrder("min", "max", "sum", "average")));
    }

    @Test
    void queryMetrics_invalidStatistic_returns400() throws Exception {
        mockMvc.perform(get("/api/v1/metrics")
                        .param("statistics", "median"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value(
                        "Invalid statistic: median. Valid values: [min, max, sum, average]"));
    }

    @Test
    void queryMetrics_onlyStartDate_returns400() throws Exception {
        mockMvc.perform(get("/api/v1/metrics")
                        .param("statistics", "average")
                        .param("startDate", "2026-04-01T00:00:00Z"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value(
                        "Both startDate and endDate must be provided together"));
    }

    @Test
    void queryMetrics_dateRangeExceeds31Days_returns400() throws Exception {
        mockMvc.perform(get("/api/v1/metrics")
                        .param("statistics", "average")
                        .param("startDate", "2026-01-01T00:00:00Z")
                        .param("endDate", "2026-04-01T00:00:00Z"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value("Date range must not exceed 31 days"));
    }

    @Test
    void queryMetrics_startAfterEnd_returns400() throws Exception {
        mockMvc.perform(get("/api/v1/metrics")
                        .param("statistics", "average")
                        .param("startDate", "2026-04-11T00:00:00Z")
                        .param("endDate", "2026-04-01T00:00:00Z"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value("startDate must be before endDate"));
    }

    @Test
    void queryMetrics_multipleStatistics_returnsNestedStructure() throws Exception {
        when(weatherMetricService.queryMetrics(any(MetricQuery.class))).thenReturn(
                Map.of("sensor-1", Map.of("temperature", Map.of("min", 20.0, "max", 30.0, "average", 25.0))));

        mockMvc.perform(get("/api/v1/metrics")
                        .param("sensorIds", "sensor-1")
                        .param("statistics", "min", "max", "average"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.statistics.length()").value(3))
                .andExpect(jsonPath("$.statistics", containsInAnyOrder("min", "max", "average")))
                .andExpect(jsonPath("$.sensorResults[0].metrics.temperature.min").value(20.0))
                .andExpect(jsonPath("$.sensorResults[0].metrics.temperature.max").value(30.0))
                .andExpect(jsonPath("$.sensorResults[0].metrics.temperature.average").value(25.0));
    }

    @Test
    void queryMetrics_commaSeparatedStatistics_parsesCorrectly() throws Exception {
        when(weatherMetricService.queryMetrics(any(MetricQuery.class))).thenReturn(
                Map.of("sensor-1", Map.of("temperature", Map.of("min", 10.0, "max", 30.0))));

        mockMvc.perform(get("/api/v1/metrics")
                        .param("statistics", "min,max"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.statistics.length()").value(2));
    }

    @Test
    void dynamoDbException_returns503() throws Exception {
        WeatherMetricRequest request = new WeatherMetricRequest("sensor-1", null, Map.of("temperature", 22.5));

        when(weatherMetricService.saveMetric(anyString(), any(), anyMap()))
                .thenThrow(DynamoDbException.builder().message("throttled").build());

        performPost(request)
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.status").value(503))
                .andExpect(jsonPath("$.error").value("Service temporarily unavailable"))
                .andExpect(jsonPath("$.message").value("A database error occurred. Please retry later."));
    }

    @Test
    void unexpectedException_returns500() throws Exception {
        WeatherMetricRequest request = new WeatherMetricRequest("sensor-1", null, Map.of("temperature", 22.5));

        when(weatherMetricService.saveMetric(anyString(), any(), anyMap())).thenThrow(new RuntimeException("unexpected"));

        performPost(request)
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.status").value(500))
                .andExpect(jsonPath("$.error").value("Internal Server Error"));
    }
}

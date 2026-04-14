package com.weathermetrics;

import com.weathermetrics.dto.WeatherMetricQueryResponse;
import com.weathermetrics.dto.WeatherMetricQueryResponse.SensorResult;
import com.weathermetrics.dto.WeatherMetricRequest;
import com.weathermetrics.model.WeatherMetric;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class WeatherMetricsE2ETest {

    private static final String SENSOR_ID = "sensor-1";

    @Container
    @SuppressWarnings("resource")
    static final GenericContainer<?> DYNAMO_LOCAL = new GenericContainer<>(DockerImageName.parse("amazon/dynamodb-local:latest"))
            .withExposedPorts(8000)
            .withCommand("-jar DynamoDBLocal.jar -inMemory -sharedDb");

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("dynamodb.endpoint", () -> "http://" + DYNAMO_LOCAL.getHost() + ":" + DYNAMO_LOCAL.getMappedPort(8000));
        registry.add("dynamodb.local", () -> "true");
        registry.add("dynamodb.region", () -> "us-east-1");
        registry.add("dynamodb.table-name", () -> "weather_metrics_e2e");
        registry.add("dynamodb.registry-table-name", () -> "weather_sensor_registry_e2e");
    }

    @Autowired
    TestRestTemplate restTemplate;

    @Test
    void postMetrics() {
        WeatherMetricRequest request = new WeatherMetricRequest(SENSOR_ID, "2026-04-10T10:00:00Z", Map.of("temperature", 22.5, "humidity", 60.0));

        ResponseEntity<WeatherMetric> response = restTemplate.postForEntity("/api/v1/metrics", request, WeatherMetric.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        WeatherMetric body = response.getBody();
        assertThat(body).isNotNull();
        assertThat(body.sensorId()).isEqualTo(SENSOR_ID);
        assertThat(body.timestamp()).isEqualTo("2026-04-10T10:00:00Z");
        assertThat(body.metrics()).containsEntry("temperature", 22.5);
        assertThat(body.metrics()).containsEntry("humidity", 60.0);
    }

    @Test
    void queryWithoutDateRange_returnsLatestRecord() {
        post("sensor-99", "2026-04-05T08:00:00Z", Map.of("temperature", 15.0));
        post("sensor-99", "2026-04-06T08:00:00Z", Map.of("temperature", 20.0));
        post("sensor-99", "2026-04-07T08:00:00Z", Map.of("temperature", 25.0));

        WeatherMetricQueryResponse body = query("/api/v1/metrics?sensorIds=sensor-99&statistics=average");

        assertThat(body.sensorResults()).hasSize(1);
        assertThat(body.sensorResults().get(0).metrics().get("temperature")).containsEntry("average", 25.0);
    }

    @Test
    void queryWithDateRange_min() {
        addMetrics();
        assertResults("min", "temperature", 10.0);
    }

    @Test
    void queryWithDateRange_max() {
        addMetrics();
        assertResults("max", "temperature", 30.0);
    }

    @Test
    void queryWithDateRange_sum() {
        addMetrics();
        assertResults("sum", "temperature", 60.0);
    }

    @Test
    void queryWithDateRange_average() {
        addMetrics();
        assertResults("average", "temperature", 20.0);
    }

    @Test
    void queryWithDateRange_filteredByMetricName() {
        post(SENSOR_ID, "2026-04-01T10:00:00Z", Map.of("temperature", 20.0, "humidity", 50.0, "pressure", 1013.0));
        post(SENSOR_ID, "2026-04-02T10:00:00Z", Map.of("temperature", 30.0, "humidity", 60.0, "pressure", 1015.0));

        WeatherMetricQueryResponse body = query("/api/v1/metrics?sensorIds=" + SENSOR_ID + "&metrics=temperature&statistics=average&startDate=2026-03-31T00:00:00Z&endDate=2026-04-03T00:00:00Z");

        assertThat(body.sensorResults()).hasSize(1);
        Map<String, Map<String, Double>> metrics = body.sensorResults().get(0).metrics();
        assertThat(metrics).containsKey("temperature");
        assertThat(metrics).doesNotContainKey("humidity");
        assertThat(metrics).doesNotContainKey("pressure");
    }

    @Test
    void queryWithDateRange_outsideWindow_returnsNoResults() {
        post(SENSOR_ID, "2026-04-20T10:00:00Z", Map.of("temperature", 99.0));

        WeatherMetricQueryResponse body = query("/api/v1/metrics?sensorIds=" + SENSOR_ID + "&statistics=average&startDate=2026-04-01T00:00:00Z&endDate=2026-04-10T00:00:00Z");

        assertThat(body.sensorResults()).isEmpty();
    }

    @Test
    void queryWithoutSensorId_returnsResultsForAllSensors() {
        post("all-A", "2026-04-12T10:00:00Z", Map.of("temperature", 15.0));
        post("all-B", "2026-04-12T10:00:00Z", Map.of("temperature", 25.0));

        WeatherMetricQueryResponse body = query("/api/v1/metrics?statistics=average&startDate=2026-04-12T00:00:00Z&endDate=2026-04-13T00:00:00Z");

        List<String> ids = body.sensorResults().stream().map(SensorResult::sensorId).toList();
        assertThat(ids).contains("all-A", "all-B");
    }

    @Test
    void queryWithDateRange_noStatistics_defaultsToAll() {
        addMetrics();

        WeatherMetricQueryResponse body = query("/api/v1/metrics?sensorIds=" + SENSOR_ID + "&startDate=2026-03-31T00:00:00Z&endDate=2026-04-04T00:00:00Z");

        assertThat(body.statistics()).containsExactlyInAnyOrder("min", "max", "sum", "average");
        Map<String, Double> tempStats = body.sensorResults().get(0).metrics().get("temperature");
        assertThat(tempStats).containsEntry("min", 10.0);
        assertThat(tempStats).containsEntry("max", 30.0);
        assertThat(tempStats).containsEntry("sum", 60.0);
        assertThat(tempStats).containsEntry("average", 20.0);
    }

    @Test
    void queryWithDateRange_multipleStatistics() {
        addMetrics();

        WeatherMetricQueryResponse body = query("/api/v1/metrics?sensorIds=" + SENSOR_ID + "&statistics=min,max,average&startDate=2026-03-31T00:00:00Z&endDate=2026-04-04T00:00:00Z");

        assertThat(body.sensorResults()).hasSize(1);
        Map<String, Double> tempStats = body.sensorResults().get(0).metrics().get("temperature");
        assertThat(tempStats).containsEntry("min", 10.0);
        assertThat(tempStats).containsEntry("max", 30.0);
        assertThat(tempStats).containsEntry("average", 20.0);
        assertThat(body.statistics()).containsExactlyInAnyOrder("min", "max", "average");
    }

    private void post(String sensorId, String timestamp, Map<String, Double> metrics) {
        restTemplate.postForEntity("/api/v1/metrics", new WeatherMetricRequest(sensorId, timestamp, metrics), WeatherMetric.class);
    }

    private void addMetrics() {
        post(SENSOR_ID, "2026-04-01T10:00:00Z", Map.of("temperature", 10.0));
        post(SENSOR_ID, "2026-04-02T10:00:00Z", Map.of("temperature", 20.0));
        post(SENSOR_ID, "2026-04-03T10:00:00Z", Map.of("temperature", 30.0));
    }

    private WeatherMetricQueryResponse query(String url) {
        ResponseEntity<WeatherMetricQueryResponse> response = restTemplate.getForEntity(url, WeatherMetricQueryResponse.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        return response.getBody();
    }

    private void assertResults(String statistic, String metric, double expected) {
        WeatherMetricQueryResponse body = query("/api/v1/metrics?sensorIds=" + SENSOR_ID + "&statistics=" + statistic + "&startDate=2026-03-31T00:00:00Z&endDate=2026-04-04T00:00:00Z");
        assertThat(body.sensorResults()).hasSize(1);
        assertThat(body.sensorResults().get(0).sensorId()).isEqualTo(SENSOR_ID);
        assertThat(body.sensorResults().get(0).metrics().get(metric)).containsEntry(statistic, expected);
    }
}

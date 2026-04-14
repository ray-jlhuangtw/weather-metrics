package com.weathermetrics.repository;

import com.weathermetrics.model.WeatherMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;

import java.util.*;
import java.util.stream.Collectors;

@Repository
public class DynamoDbWeatherMetricRepository implements WeatherMetricRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDbWeatherMetricRepository.class);

    private final DynamoDbClient dynamoDbClient;
    private final String tableName;
    private final String registryTableName;

    public DynamoDbWeatherMetricRepository(
            DynamoDbClient dynamoDbClient,
            @Value("${dynamodb.table-name}") String tableName,
            @Value("${dynamodb.registry-table-name}") String registryTableName) {
        this.dynamoDbClient = dynamoDbClient;
        this.tableName = tableName;
        this.registryTableName = registryTableName;
    }

    @Override
    public void save(WeatherMetric metric) {
        LOGGER.debug("Saving metric for sensor={} at timestamp={}", metric.sensorId(), metric.timestamp());

        Map<String, AttributeValue> item = new HashMap<>();
        item.put("sensorId", s(metric.sensorId()));
        item.put("timestamp", s(metric.timestamp()));

        Map<String, AttributeValue> metricsMap = new HashMap<>();
        metric.metrics().forEach((key, value) -> metricsMap.put(key, n(value)));
        item.put("metrics", m(metricsMap));

        dynamoDbClient.putItem(PutItemRequest.builder().tableName(tableName).item(item).build());
        dynamoDbClient.putItem(PutItemRequest.builder()
                .tableName(registryTableName)
                .item(Map.of("sensorId", s(metric.sensorId())))
                .build());
    }

    @Override
    public List<WeatherMetric> queryBySensorAndTimeRange(String sensorId, String startTime, String endTime) {
        LOGGER.debug("Querying sensor={} from {} to {}", sensorId, startTime, endTime);
        QueryRequest request = QueryRequest.builder()
                .tableName(tableName)
                .keyConditionExpression("sensorId = :sid AND #ts BETWEEN :start AND :end")
                .expressionAttributeNames(Map.of("#ts", "timestamp"))
                .expressionAttributeValues(Map.of(":sid", s(sensorId), ":start", s(startTime), ":end", s(endTime)))
                .build();

        return dynamoDbClient.queryPaginator(request).items().stream()
                .map(this::toWeatherMetric)
                .collect(Collectors.toList());
    }

    @Override
    public Optional<WeatherMetric> getLatestBySensor(String sensorId) {
        QueryRequest request = QueryRequest.builder()
                .tableName(tableName)
                .keyConditionExpression("sensorId = :sid")
                .expressionAttributeValues(Map.of(":sid", s(sensorId)))
                .scanIndexForward(false)
                .limit(1)
                .build();

        List<Map<String, AttributeValue>> items = dynamoDbClient.query(request).items();
        return items.isEmpty() ? Optional.empty() : Optional.of(toWeatherMetric(items.get(0)));
    }

    @Override
    public Set<String> getAllSensorIds() {
        return dynamoDbClient.scanPaginator(ScanRequest.builder().tableName(registryTableName).projectionExpression("sensorId").build())
                .items().stream()
                .map(item -> item.get("sensorId").s())
                .collect(Collectors.toSet());
    }

    private WeatherMetric toWeatherMetric(Map<String, AttributeValue> item) {
        String sensorId = item.get("sensorId").s();
        String timestamp = item.get("timestamp").s();

        Map<String, Double> metrics = new HashMap<>();
        if (item.containsKey("metrics") && item.get("metrics").m() != null) {
            item.get("metrics").m().forEach((key, value) -> {
                if (value.n() != null) {
                    metrics.put(key, Double.parseDouble(value.n()));
                } else {
                    LOGGER.warn("Skipping non-numeric metric attribute '{}' for sensor={}", key, sensorId);
                }
            });
        }
        return new WeatherMetric(sensorId, timestamp, metrics);
    }

    private static AttributeValue s(String value) {
        return AttributeValue.builder().s(value).build();
    }

    private static AttributeValue n(double value) {
        return AttributeValue.builder().n(String.valueOf(value)).build();
    }

    private static AttributeValue m(Map<String, AttributeValue> map) {
        return AttributeValue.builder().m(map).build();
    }
}

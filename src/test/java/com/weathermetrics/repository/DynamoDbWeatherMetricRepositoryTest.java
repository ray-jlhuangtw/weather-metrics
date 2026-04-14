package com.weathermetrics.repository;

import com.weathermetrics.model.WeatherMetric;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class DynamoDbWeatherMetricRepositoryTest {

    private static final String SENSOR_1 = "sensor-1";

    private static final String TABLE_NAME = "sensor_metrics_test";
    private static final String REGISTRY_TABLE_NAME = "sensor_registry_test";

    @Container
    @SuppressWarnings("resource")
    static final GenericContainer<?> DYNAMO_LOCAL = new GenericContainer<>(DockerImageName.parse("amazon/dynamodb-local:latest"))
            .withExposedPorts(8000)
            .withCommand("-jar DynamoDBLocal.jar -inMemory -sharedDb");
    public static final String SENSOR_2 = "sensor-2";

    private static DynamoDbClient dynamoDbClient;
    private WeatherMetricRepository weatherMetricRepository;

    @BeforeAll
    static void startClient() {
        String endpoint = "http://" + DYNAMO_LOCAL.getHost() + ":" + DYNAMO_LOCAL.getMappedPort(8000);

        dynamoDbClient = DynamoDbClient.builder()
                .endpointOverride(URI.create(endpoint))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("local", "local")))
                .build();

        createMetricsTable();
        createRegistryTable();
    }

    @BeforeEach
    void setUp() {
        weatherMetricRepository = new DynamoDbWeatherMetricRepository(dynamoDbClient, TABLE_NAME, REGISTRY_TABLE_NAME);
        cleanTable(TABLE_NAME, "sensorId", "timestamp");
        cleanRegistryTable();
    }

    @AfterAll
    static void stopClient() {
        if (dynamoDbClient != null) {
            dynamoDbClient.close();
        }
    }

    @Test
    void save_and_queryByTimeRange() {
        weatherMetricRepository.save(new WeatherMetric(SENSOR_1, "2026-04-05T10:00:00Z", Map.of("temperature", 22.0, "humidity", 45.0)));
        weatherMetricRepository.save(new WeatherMetric(SENSOR_1, "2026-04-06T10:00:00Z", Map.of("temperature", 24.0, "humidity", 50.0)));
        weatherMetricRepository.save(new WeatherMetric(SENSOR_1, "2026-04-15T10:00:00Z", Map.of("temperature", 30.0)));

        List<WeatherMetric> results = weatherMetricRepository.queryBySensorAndTimeRange(SENSOR_1, "2026-04-01T00:00:00Z", "2026-04-10T00:00:00Z");

        assertThat(results).hasSize(2);
        assertThat(results.get(0).metrics()).containsEntry("temperature", 22.0);
        assertThat(results.get(1).metrics()).containsEntry("temperature", 24.0);
    }

    @Test
    void getLatestBySensor_returnsLatest() {
        weatherMetricRepository.save(new WeatherMetric(SENSOR_1, "2026-04-05T10:00:00Z", Map.of("temperature", 22.0)));
        weatherMetricRepository.save(new WeatherMetric(SENSOR_1, "2026-04-06T10:00:00Z", Map.of("temperature", 24.0)));

        Optional<WeatherMetric> latest = weatherMetricRepository.getLatestBySensor(SENSOR_1);

        assertThat(latest).isPresent();
        assertThat(latest.get().timestamp()).isEqualTo("2026-04-06T10:00:00Z");
        assertThat(latest.get().metrics()).containsEntry("temperature", 24.0);
    }

    @Test
    void queryByTimeRange_differentSensorsNotMixed() {
        weatherMetricRepository.save(new WeatherMetric(SENSOR_1, "2026-04-05T10:00:00Z", Map.of("temperature", 22.0)));
        weatherMetricRepository.save(new WeatherMetric(SENSOR_2, "2026-04-05T10:00:00Z", Map.of("temperature", 28.0)));

        List<WeatherMetric> results = weatherMetricRepository.queryBySensorAndTimeRange(SENSOR_1, "2026-04-01T00:00:00Z", "2026-04-10T00:00:00Z");

        assertThat(results).hasSize(1);
        assertThat(results.get(0).sensorId()).isEqualTo(SENSOR_1);
    }

    @Test
    void queryByTimeRange_emptyWhenNoMatchingRecords() {
        weatherMetricRepository.save(new WeatherMetric(SENSOR_1, "2026-04-15T10:00:00Z", Map.of("temperature", 22.0)));

        List<WeatherMetric> results = weatherMetricRepository.queryBySensorAndTimeRange(SENSOR_1, "2026-04-01T00:00:00Z", "2026-04-10T00:00:00Z");

        assertThat(results).isEmpty();
    }

    @Test
    void save_overwritesExistingRecordWithSameKey() {
        weatherMetricRepository.save(new WeatherMetric(SENSOR_1, "2026-04-05T10:00:00Z", Map.of("temperature", 20.0)));
        weatherMetricRepository.save(new WeatherMetric(SENSOR_1, "2026-04-05T10:00:00Z", Map.of("temperature", 99.0)));

        List<WeatherMetric> results = weatherMetricRepository.queryBySensorAndTimeRange(SENSOR_1, "2026-04-01T00:00:00Z", "2026-04-10T00:00:00Z");

        assertThat(results).hasSize(1);
        assertThat(results.get(0).metrics()).containsEntry("temperature", 99.0);
    }

    @Test
    void queryByTimeRange_resultsOrderedByTimestampAscending() {
        weatherMetricRepository.save(new WeatherMetric(SENSOR_1, "2026-04-07T10:00:00Z", Map.of("temperature", 30.0)));
        weatherMetricRepository.save(new WeatherMetric(SENSOR_1, "2026-04-05T10:00:00Z", Map.of("temperature", 20.0)));
        weatherMetricRepository.save(new WeatherMetric(SENSOR_1, "2026-04-06T10:00:00Z", Map.of("temperature", 25.0)));

        List<WeatherMetric> results = weatherMetricRepository.queryBySensorAndTimeRange(SENSOR_1, "2026-04-01T00:00:00Z", "2026-04-10T00:00:00Z");

        assertThat(results).hasSize(3);
        assertThat(results.get(0).timestamp()).isEqualTo("2026-04-05T10:00:00Z");
        assertThat(results.get(1).timestamp()).isEqualTo("2026-04-06T10:00:00Z");
        assertThat(results.get(2).timestamp()).isEqualTo("2026-04-07T10:00:00Z");
    }

    @Test
    void getAllSensorIds_returnsDistinctSensors() {
        weatherMetricRepository.save(new WeatherMetric(SENSOR_1, "2026-04-05T10:00:00Z", Map.of("temperature", 22.0)));
        weatherMetricRepository.save(new WeatherMetric(SENSOR_1, "2026-04-06T10:00:00Z", Map.of("temperature", 24.0)));
        weatherMetricRepository.save(new WeatherMetric(SENSOR_2, "2026-04-05T10:00:00Z", Map.of("temperature", 28.0)));

        Set<String> sensorIds = weatherMetricRepository.getAllSensorIds();

        assertThat(sensorIds).containsExactlyInAnyOrder(SENSOR_1, SENSOR_2);
    }

    private static void createMetricsTable() {
        dynamoDbClient.createTable(CreateTableRequest.builder()
                .tableName(TABLE_NAME)
                .keySchema(KeySchemaElement.builder().attributeName("sensorId").keyType(KeyType.HASH).build(),
                        KeySchemaElement.builder().attributeName("timestamp").keyType(KeyType.RANGE).build())
                .attributeDefinitions(AttributeDefinition.builder().attributeName("sensorId").attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName("timestamp").attributeType(ScalarAttributeType.S).build())
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .build());

        try (DynamoDbWaiter waiter = DynamoDbWaiter.builder().client(dynamoDbClient).build()) {
            waiter.waitUntilTableExists(r -> r.tableName(TABLE_NAME));
        }
    }

    private static void createRegistryTable() {
        dynamoDbClient.createTable(CreateTableRequest.builder()
                .tableName(REGISTRY_TABLE_NAME)
                .keySchema(KeySchemaElement.builder().attributeName("sensorId").keyType(KeyType.HASH).build())
                .attributeDefinitions(AttributeDefinition.builder().attributeName("sensorId").attributeType(ScalarAttributeType.S).build())
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .build());

        try (DynamoDbWaiter waiter = DynamoDbWaiter.builder().client(dynamoDbClient).build()) {
            waiter.waitUntilTableExists(r -> r.tableName(REGISTRY_TABLE_NAME));
        }
    }

    private static void cleanTable(String name, String hashKey, String rangeKey) {
        ScanResponse scan = dynamoDbClient.scan(ScanRequest.builder().tableName(name).build());
        for (Map<String, AttributeValue> item : scan.items()) {
            dynamoDbClient.deleteItem(DeleteItemRequest.builder()
                    .tableName(name)
                    .key(Map.of(hashKey, item.get(hashKey), rangeKey, item.get(rangeKey)))
                    .build());
        }
    }

    private static void cleanRegistryTable() {
        ScanResponse scan = dynamoDbClient.scan(ScanRequest.builder().tableName(REGISTRY_TABLE_NAME).build());
        for (Map<String, AttributeValue> item : scan.items()) {
            dynamoDbClient.deleteItem(DeleteItemRequest.builder()
                    .tableName(REGISTRY_TABLE_NAME)
                    .key(Map.of("sensorId", item.get("sensorId")))
                    .build());
        }
    }
}

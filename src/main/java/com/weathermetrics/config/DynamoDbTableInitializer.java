package com.weathermetrics.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter;

@Component
public class DynamoDbTableInitializer implements ApplicationRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDbTableInitializer.class);

    private final DynamoDbClient dynamoDbClient;
    private final String tableName;
    private final String registryTableName;

    public DynamoDbTableInitializer(
            DynamoDbClient dynamoDbClient,
            @Value("${dynamodb.table-name}") String tableName,
            @Value("${dynamodb.registry-table-name}") String registryTableName) {
        this.dynamoDbClient = dynamoDbClient;
        this.tableName = tableName;
        this.registryTableName = registryTableName;
    }

    @Override
    public void run(ApplicationArguments args) {
        ensureMetricsTable();
        ensureRegistryTable();
    }

    private void ensureMetricsTable() {
        if (tableExists(tableName)) {
            LOGGER.info("Table '{}' already exists", tableName);
            return;
        }
        LOGGER.info("Creating table '{}'", tableName);
        dynamoDbClient.createTable(CreateTableRequest.builder()
                .tableName(tableName)
                .keySchema(KeySchemaElement.builder().attributeName("sensorId").keyType(KeyType.HASH).build(),
                        KeySchemaElement.builder().attributeName("timestamp").keyType(KeyType.RANGE).build())
                .attributeDefinitions(AttributeDefinition.builder().attributeName("sensorId").attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName("timestamp").attributeType(ScalarAttributeType.S).build())
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .build());
        waitUntilActive(tableName);
    }

    private void ensureRegistryTable() {
        if (tableExists(registryTableName)) {
            LOGGER.info("Registry table '{}' already exists", registryTableName);
            return;
        }
        LOGGER.info("Creating registry table '{}'", registryTableName);
        dynamoDbClient.createTable(CreateTableRequest.builder()
                .tableName(registryTableName)
                .keySchema(KeySchemaElement.builder().attributeName("sensorId").keyType(KeyType.HASH).build())
                .attributeDefinitions(AttributeDefinition.builder().attributeName("sensorId").attributeType(ScalarAttributeType.S).build())
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .build());
        waitUntilActive(registryTableName);
    }

    private boolean tableExists(String name) {
        try {
            dynamoDbClient.describeTable(DescribeTableRequest.builder().tableName(name).build());
            return true;
        } catch (ResourceNotFoundException e) {
            return false;
        }
    }

    private void waitUntilActive(String name) {
        LOGGER.info("Waiting for table '{}' to become ACTIVE...", name);
        try (DynamoDbWaiter waiter = DynamoDbWaiter.builder().client(dynamoDbClient).build()) {
            waiter.waitUntilTableExists(r -> r.tableName(name));
        }
        LOGGER.info("Table '{}' is ACTIVE", name);
    }
}

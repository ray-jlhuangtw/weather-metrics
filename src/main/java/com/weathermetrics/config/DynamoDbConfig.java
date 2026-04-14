package com.weathermetrics.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.net.URI;

@Configuration
public class DynamoDbConfig {

    @Bean
    public DynamoDbClient dynamoDbClient(
            @Value("${dynamodb.region}") String region,
            @Value("${dynamodb.local:false}") boolean local,
            @Value("${dynamodb.endpoint:}") String endpoint) {

        var builder = DynamoDbClient.builder()
                .region(Region.of(region));

        if (local) {
            builder.endpointOverride(URI.create(endpoint))
                    .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
                            System.getenv().getOrDefault("DYNAMODB_ACCESS_KEY", "local"),
                            System.getenv().getOrDefault("DYNAMODB_SECRET_KEY", "local"))));
        } else {
            builder.credentialsProvider(DefaultCredentialsProvider.create());
        }

        return builder.build();
    }
}

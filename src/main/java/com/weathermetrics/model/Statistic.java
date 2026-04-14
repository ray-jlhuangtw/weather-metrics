package com.weathermetrics.model;

import java.util.Collections;
import java.util.List;

public enum Statistic {

    MIN {
        @Override
        public double compute(List<Double> values) {
            return values.stream().mapToDouble(Double::doubleValue).min().orElse(0);
        }
    },
    MAX {
        @Override
        public double compute(List<Double> values) {
            return values.stream().mapToDouble(Double::doubleValue).max().orElse(0);
        }
    },
    SUM {
        @Override
        public double compute(List<Double> values) {
            return values.stream().mapToDouble(Double::doubleValue).sum();
        }
    },
    AVERAGE {
        @Override
        public double compute(List<Double> values) {
            return values.stream().mapToDouble(Double::doubleValue).average().orElse(0);
        }
    };

    public abstract double compute(List<Double> values);

    public static Statistic fromString(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("statistic is required. Valid values: [min, max, sum, average]");
        }
        try {
            return Statistic.valueOf(value.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid statistic: " + value + ". Valid values: [min, max, sum, average]");
        }
    }

    public static List<Statistic> fromStrings(List<String> values) {
        if (values == null || values.isEmpty()) {
            return Collections.emptyList();
        }
        return values.stream().map(Statistic::fromString).toList();
    }
}

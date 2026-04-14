package com.weathermetrics.dto;

import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;

public record TimeRange(Instant start, Instant end) {

    private static final long MAX_RANGE_DAYS = 31;

    public TimeRange {
        if (start.isAfter(end)) {
            throw new IllegalArgumentException("startDate must be before endDate");
        }
        if (Duration.between(start, end).compareTo(Duration.ofDays(MAX_RANGE_DAYS)) > 0) {
            throw new IllegalArgumentException("Date range must not exceed " + MAX_RANGE_DAYS + " days");
        }
    }

    public static TimeRange of(String start, String end) {
        return new TimeRange(parseInstant(start, "startDate"), parseInstant(end, "endDate"));
    }

    public String startAsString() {
        return start.toString();
    }

    public String endAsString() {
        return end.toString();
    }

    private static Instant parseInstant(String value, String fieldName) {
        try {
            return Instant.parse(value);
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("Invalid " + fieldName + " format.");
        }
    }
}

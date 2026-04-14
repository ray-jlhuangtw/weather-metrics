package com.weathermetrics.dto;

import com.weathermetrics.model.Statistic;

import java.util.List;

public record MetricQuery(List<String> sensorIds, List<String> metricNames, List<Statistic> statistics, TimeRange timeRange) {

    public boolean hasDateRange() {
        return timeRange != null;
    }
}

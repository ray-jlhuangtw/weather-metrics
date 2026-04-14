### Start local infrastructure

```bash
docker-compose up -d
```
### Run the application

```bash
mvn spring-boot:run -Dspring-boot.run.profiles=local
```

The API is available at `http://localhost:8080`.

On startup, the application automatically creates the required DynamoDB tables if they do not exist:
- `weather_metrics` — stores sensor readings

### Running Tests

```bash
mvn clean test
```

### API Reference

### POST `/api/v1/metrics` — Ingest a sensor reading

Records a metric value from a sensor.

**Request body:**
```json
{
  "sensorId": "sensor-1",
  "metrics": {
    "temperature": 23.5,
    "humidity": 65.0
  },
  "timestamp": "2026-04-12T10:30:00Z"
}
```

**curl example:**
```bash
curl -s -X POST http://localhost:8080/api/v1/metrics \
  -H "Content-Type: application/json" \
  -d '{
    "sensorId": "sensor-1",
    "metrics": { "temperature": 23.5, "humidity": 65.0 },
    "timestamp": "2026-04-12T10:30:00Z"
  }'
```

### GET `/api/v1/metrics` — Query metrics with statistics

**curl examples:**

Average temperature for sensor-1 over the last week:
```bash
curl -s "http://localhost:8080/api/v1/metrics?sensorIds=sensor-1&metrics=temperature&statistics=average&startDate=2026-04-05T00:00:00Z&endDate=2026-04-12T00:00:00Z"
```

Min, max, and average across all sensors, all metrics, over a date range:
```bash
curl -s "http://localhost:8080/api/v1/metrics?statistics=min,max,average&startDate=2026-04-05T00:00:00Z&endDate=2026-04-12T00:00:00Z"
```

Max reading across all sensors (latest reading only):
```bash
curl -s "http://localhost:8080/api/v1/metrics?statistics=max"
```

Sum of humidity for multiple sensors:
```bash
curl -s "http://localhost:8080/api/v1/metrics?sensorIds=sensor-1,sensor-2&metrics=humidity&statistics=sum&startDate=2026-04-01T00:00:00Z&endDate=2026-04-12T00:00:00Z"
```

Min and max together using repeated params:
```bash
curl -s "http://localhost:8080/api/v1/metrics?sensorIds=sensor-1&statistics=min&statistics=max&startDate=2026-04-01T00:00:00Z&endDate=2026-04-12T00:00:00Z"
```

**Response examples:**
```json
{
  "queriedSensorIds": ["sensor-1"],
  "statistics": ["min", "max", "average"],
  "startDate": "2026-04-05T00:00:00Z",
  "endDate": "2026-04-12T00:00:00Z",
  "sensorResults": [
    {
      "sensorId": "sensor-1",
      "metrics": {
        "temperature": { "min": 18.0, "max": 27.5, "average": 22.8 },
        "humidity":    { "min": 55.0, "max": 80.0, "average": 66.3 }
      }
    }
  ]
}
```
# Prometheus Metrics Setup

## Metrics Added

### 1. UP Metric (For API Down Alert)
- **Metric**: `up{job="NHIT_RAMS_api_health"}`
- **Type**: Gauge
- **Values**: 1 (up) or 0 (down)
- **Description**: Service health status

### 2. HTTP Request Counter
- **Metric**: `http_requests_total`
- **Type**: Counter
- **Labels**: method, endpoint, status
- **Description**: Total number of HTTP requests

### 3. HTTP Request Latency (For High Latency Alert)
- **Metric**: `http_request_duration_seconds`
- **Type**: Histogram
- **Labels**: endpoint
- **Description**: Request duration in seconds (for latency monitoring)

## Endpoints

### `/metrics`
Returns Prometheus-formatted metrics.

**Example:**
```bash
curl https://your-app.railway.app/metrics
```

**Output:**
```
# HELP up Service is up (1) or down (0)
# TYPE up gauge
up{job="NHIT_RAMS_api_health"} 1.0

# HELP http_requests_total Total HTTP Requests
# TYPE http_requests_total counter
http_requests_total{endpoint="/health",method="GET",status="200"} 5.0

# HELP http_request_duration_seconds Request latency
# TYPE http_request_duration_seconds histogram
http_request_duration_seconds_bucket{endpoint="/health",le="0.005"} 3.0
http_request_duration_seconds_bucket{endpoint="/health",le="0.01"} 5.0
http_request_duration_seconds_bucket{endpoint="/health",le="2.5"} 5.0
http_request_duration_seconds_sum{endpoint="/health"} 0.025
http_request_duration_seconds_count{endpoint="/health"} 5.0
```

### `/health`
Health check endpoint for Railway.

**Example:**
```bash
curl https://your-app.railway.app/health
```

**Response:**
```json
{
  "status": "healthy",
  "service": "NHIT_RAMS_API",
  "scheduler_running": true,
  "active_jobs": 2
}
```

## Prometheus Configuration

Add this to your `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: "NHIT_RAMS_api"
    scheme: https
    metrics_path: /metrics
    static_configs:
      - targets:
          - "fantastic1-production.up.railway.app"
```

## Alert Rules

### For API Down Alert
```yaml
- alert: NHITRAMSAPIDown
  expr: up{job="NHIT_RAMS_api"} == 0
  for: 1m
  labels:
    severity: critical
  annotations:
    summary: "🚨 NHIT_RAMS API Down"
    description: "NHIT_RAMS API is not responding"
```

### For High Latency Alert
```yaml
- alert: NHITRAMSHighLatency
  expr: |
    histogram_quantile(0.95,
      rate(http_request_duration_seconds_bucket[5m])
    ) > 2
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "⚠️ High Latency (NHIT_RAMS API)"
    description: "NHIT_RAMS API responding slowly"
```

## How It Works

1. **Automatic Tracking**: The middleware automatically tracks all HTTP requests
2. **Metrics Collection**: Every request increments counters and records latency
3. **Prometheus Scraping**: Prometheus scrapes `/metrics` endpoint periodically
4. **Alerting**: Alerts fire based on your configured rules

## Testing

```bash
# Deploy to Railway
git add .
git commit -m "Add Prometheus metrics"
git push

# Test metrics endpoint
curl https://fantastic1-production.up.railway.app/metrics

# Test health endpoint
curl https://fantastic1-production.up.railway.app/health

# Generate some traffic
curl https://fantastic1-production.up.railway.app/inventory_filter

# Check metrics again
curl https://fantastic1-production.up.railway.app/metrics | grep http_requests_total
```

## What Gets Tracked

- All HTTP requests (GET, POST, etc.)
- Response status codes (200, 404, 500, etc.)
- Request duration/latency
- Per-endpoint metrics

## Benefits

✅ Monitor API availability (up/down)
✅ Track request latency and performance
✅ Identify slow endpoints
✅ Alert on issues automatically
✅ Historical metrics for analysis

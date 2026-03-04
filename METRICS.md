# Prometheus Metrics Documentation

## Available Metrics (Aligned with Alert Rules)

### Application Health (For Alert: NHITRAMSAPIDown)
- `up{job="NHIT_RAMS_api_health"}` (Gauge) - Application status
  - **1** = Application is UP and healthy
  - **0** = Application is DOWN
  - **Alert triggers when**: `up{job="NHIT_RAMS_api_health"} == 0` for 1 minute

### HTTP Request Duration (For Alert: NHITRAMSHighLatency)
- `http_request_duration_seconds` (Histogram) - Request latency with buckets
  - Tracks 95th percentile latency
  - **Alert triggers when**: P95 latency > 2 seconds for 5 minutes
  - Buckets: 0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0

### Additional Metrics

#### HTTP Metrics
- `http_requests_total` (Counter) - Total HTTP requests by method, endpoint, and status

#### Image Processing
- `images_processed_total` (Counter) - Total images processed by operation type
  - Labels: `operation_type` (process_image, upload_images, etc.)

#### Excel Operations
- `excel_operations_total` (Counter) - Total Excel operations
  - Labels: `operation` (distress_append, inventory_append)

#### Scheduler
- `scheduled_jobs_active` (Gauge) - Number of active scheduled jobs

#### Cache
- `cache_entries` (Gauge) - Number of entries in cache by type
  - Labels: `cache_type` (main, images, profiles, ri)

#### Errors
- `errors_total` (Counter) - Total errors by type
  - Labels: `error_type` (invalid_filename, process_image_error, etc.)

## Endpoints

### `/metrics`
Returns Prometheus-formatted metrics in text format.

**Example:**
```bash
curl https://fantastic1-production.up.railway.app/metrics
```

**Sample Output:**
```
# HELP up Application is running (1 = up, 0 = down)
# TYPE up gauge
up{job="NHIT_RAMS_api_health"} 1.0

# HELP http_request_duration_seconds HTTP request duration in seconds
# TYPE http_request_duration_seconds histogram
http_request_duration_seconds_bucket{endpoint="/health",method="GET",le="0.005"} 10.0
http_request_duration_seconds_bucket{endpoint="/health",method="GET",le="0.01"} 15.0
http_request_duration_seconds_bucket{endpoint="/health",method="GET",le="+Inf"} 20.0
http_request_duration_seconds_sum{endpoint="/health",method="GET"} 0.15
http_request_duration_seconds_count{endpoint="/health",method="GET"} 20.0
```

### `/health`
Returns JSON health check status.

**Example:**
```bash
curl https://fantastic1-production.up.railway.app/health
```

**Response:**
```json
{
  "status": "healthy",
  "app_up": 1,
  "scheduler_running": true,
  "active_jobs": 2
}
```

## Prometheus Configuration

Your `prometheus.yml` scrape config:
```yaml
- job_name: "NHIT_RAMS_api"
  scheme: https
  metrics_path: /metrics
  static_configs:
    - targets:
        - "fantastic1-production.up.railway.app"
```

## Alert Rules

### 1. NHITRAMSAPIDown
```yaml
- alert: NHITRAMSAPIDown
  expr: up{job="NHIT_RAMS_api_health"} == 0
  for: 1m
  labels:
    severity: critical
  annotations:
    summary: "🚨 NHIT_RAMS API Down"
    description: "NHIT_RAMS API is not responding"
```

**What it monitors**: Checks if the `up` metric equals 0 (app is down)

**When it fires**: After 1 minute of the app being down

### 2. NHITRAMSHighLatency
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

**What it monitors**: 95th percentile of request duration over 5 minutes

**When it fires**: When P95 latency exceeds 2 seconds for 5 minutes

## How Metrics Work

1. **Automatic Tracking**: All HTTP requests are automatically tracked via middleware
2. **Duration Buckets**: Request durations are recorded in histogram buckets for percentile calculations
3. **Health Status**: The `up` metric is set to 1 on startup and 0 on shutdown
4. **Custom Metrics**: Image processing and Excel operations are tracked separately

## Testing Locally

```bash
# Install dependencies
pip install -r requirements.txt

# Run the app
uvicorn main:app --host 0.0.0.0 --port 8000

# Check metrics
curl http://localhost:8000/metrics | grep "up{"
curl http://localhost:8000/metrics | grep "http_request_duration"

# Check health
curl http://localhost:8000/health

# Generate some traffic to see metrics
curl -X POST http://localhost:8000/process_image -F "file=@test_image.jpg"
```

## Troubleshooting

### Alert not firing?
1. Check if Prometheus can scrape `/metrics`: `curl https://your-app.railway.app/metrics`
2. Verify `up{job="NHIT_RAMS_api_health"}` shows value 1
3. Check Prometheus targets page to ensure scraping is successful

### High latency alert?
1. Check current P95 latency: Query `histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m]))`
2. Identify slow endpoints: Check `http_request_duration_seconds` by endpoint label
3. Optimize slow endpoints or increase alert threshold


# Prometheus Metrics Documentation

## Available Metrics

### Application Health
- `app_up` (Gauge) - Application status (1 = up, 0 = down)
- `app_info` (Info) - Application version and name

### HTTP Metrics
- `http_requests_total` (Counter) - Total HTTP requests by method, endpoint, and status
- `http_request_duration_seconds` (Histogram) - Request duration in seconds

### Image Processing
- `images_processed_total` (Counter) - Total images processed by operation type
  - Labels: `operation_type` (process_image, upload_images, etc.)

### Excel Operations
- `excel_operations_total` (Counter) - Total Excel operations
  - Labels: `operation` (distress_append, inventory_append)

### Scheduler
- `scheduled_jobs_active` (Gauge) - Number of active scheduled jobs

### Cache
- `cache_entries` (Gauge) - Number of entries in cache by type
  - Labels: `cache_type` (main, images, profiles, ri)

### Errors
- `errors_total` (Counter) - Total errors by type
  - Labels: `error_type` (invalid_filename, process_image_error, etc.)

## Endpoints

### `/metrics`
Returns Prometheus-formatted metrics in text format.

**Example:**
```
curl https://your-app.railway.app/metrics
```

### `/health`
Returns JSON health check status.

**Example:**
```json
{
  "status": "healthy",
  "app_up": 1,
  "scheduler_running": true,
  "active_jobs": 2
}
```

## Railway Configuration

The `railway.toml` file configures Railway to:
- Use `/health` for health checks
- Scrape `/metrics` for Prometheus metrics

Railway will automatically:
1. Monitor the `app_up` metric (1 = healthy, 0 = down)
2. Collect all custom metrics
3. Display them in the Railway dashboard

## Testing Locally

```bash
# Install dependencies
pip install -r requirements.txt

# Run the app
uvicorn main:app --host 0.0.0.0 --port 8000

# Check metrics
curl http://localhost:8000/metrics

# Check health
curl http://localhost:8000/health
```

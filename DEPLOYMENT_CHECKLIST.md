# Railway Deployment Checklist

## ✅ Changes Made

### 1. Added Prometheus Metrics
- ✅ `up{job="NHIT_RAMS_api_health"}` - For NHITRAMSAPIDown alert
- ✅ `http_request_duration_seconds` - For NHITRAMSHighLatency alert (with histogram buckets)
- ✅ Additional custom metrics for monitoring

### 2. Files Modified
- ✅ `requirements.txt` - Added `prometheus-client==0.19.0`
- ✅ `main.py` - Added metrics collection and middleware
- ✅ `railway.toml` - Configured metrics path
- ✅ `METRICS.md` - Documentation

### 3. Endpoints Added
- ✅ `/metrics` - Prometheus metrics endpoint (text format)
- ✅ `/health` - Health check endpoint (JSON format)

## 🚀 Deployment Steps

1. **Commit changes:**
   ```bash
   git add .
   git commit -m "Add Prometheus metrics for Railway monitoring"
   git push
   ```

2. **Railway will auto-deploy**

3. **Verify deployment:**
   ```bash
   # Check metrics endpoint
   curl https://fantastic1-production.up.railway.app/metrics
   
   # Should see:
   # up{job="NHIT_RAMS_api_health"} 1.0
   # http_request_duration_seconds_bucket{...} ...
   
   # Check health endpoint
   curl https://fantastic1-production.up.railway.app/health
   ```

## 🔍 Verify Alerts Work

### Test Alert 1: NHITRAMSAPIDown
```yaml
expr: up{job="NHIT_RAMS_api_health"} == 0
for: 1m
```

**How to verify:**
1. Check Prometheus targets: Should show "UP"
2. Query in Prometheus: `up{job="NHIT_RAMS_api_health"}`
3. Should return: `1` (healthy)
4. Alert should be GREEN (not firing)

### Test Alert 2: NHITRAMSHighLatency
```yaml
expr: histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m])) > 2
for: 5m
```

**How to verify:**
1. Query in Prometheus: `histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m]))`
2. Should return current P95 latency (e.g., 0.15 seconds)
3. Alert should be GREEN if latency < 2 seconds

## 📊 Prometheus Configuration

Your `prometheus.yml` should have:
```yaml
scrape_configs:
  - job_name: "NHIT_RAMS_api"
    scheme: https
    metrics_path: /metrics
    static_configs:
      - targets:
          - "fantastic1-production.up.railway.app"
```

## 🎯 What Each Alert Does

### NHITRAMSAPIDown (Critical)
- **Monitors**: Application availability
- **Fires when**: App is down for 1 minute
- **Metric**: `up{job="NHIT_RAMS_api_health"} == 0`
- **Action**: Immediate investigation required

### NHITRAMSHighLatency (Warning)
- **Monitors**: Response time performance
- **Fires when**: P95 latency > 2 seconds for 5 minutes
- **Metric**: `histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m]))`
- **Action**: Check slow endpoints, optimize if needed

## 🐛 Troubleshooting

### Metrics endpoint returns 404
- Check `railway.toml` is deployed
- Verify `/metrics` endpoint exists: `curl https://your-app/metrics`

### Alert always firing
- Check if app is actually down: `curl https://your-app/health`
- Verify Prometheus can scrape: Check Prometheus targets page
- Check metric value: `up{job="NHIT_RAMS_api_health"}`

### No data in Prometheus
- Verify scrape config job name matches: `NHIT_RAMS_api`
- Check Prometheus logs for scrape errors
- Ensure Railway app is publicly accessible

## 📈 Monitoring Dashboard

After deployment, you can query these in Prometheus:

```promql
# Check if app is up
up{job="NHIT_RAMS_api_health"}

# Current request rate
rate(http_requests_total[5m])

# P95 latency
histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m]))

# Error rate
rate(errors_total[5m])

# Images processed
rate(images_processed_total[5m])
```

## ✨ Success Criteria

- [ ] `/metrics` endpoint returns Prometheus format data
- [ ] `/health` endpoint returns JSON with status
- [ ] `up{job="NHIT_RAMS_api_health"}` shows value `1`
- [ ] `http_request_duration_seconds_bucket` has data
- [ ] Prometheus successfully scrapes the target
- [ ] Both alerts are GREEN (not firing)

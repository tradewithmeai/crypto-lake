# Deployment Guide

This directory contains deployment configuration files for running Crypto Lake on cloud infrastructure.

## Systemd Service (Linux/GCP)

### Installation

1. Copy the service file to systemd directory:
```bash
sudo cp deploy/crypto-lake.service /etc/systemd/system/
```

2. Create environment file:
```bash
sudo cp deploy/crypto-lake.env.example /etc/default/crypto-lake
sudo chmod 600 /etc/default/crypto-lake
sudo nano /etc/default/crypto-lake  # Edit with your values
```

3. Ensure data directories exist with correct permissions:
```bash
sudo mkdir -p /data/{raw,parquet,logs/qa,logs/health,reports}
sudo chown -R Eschaton:Eschaton /data
```

4. Reload systemd and enable service:
```bash
sudo systemctl daemon-reload
sudo systemctl enable crypto-lake
sudo systemctl start crypto-lake
```

### Service Management

```bash
# Check status
sudo systemctl status crypto-lake

# View logs
sudo journalctl -u crypto-lake -f

# Restart service
sudo systemctl restart crypto-lake

# Stop service
sudo systemctl stop crypto-lake

# Disable auto-start
sudo systemctl disable crypto-lake
```

### Troubleshooting

**Service fails to start:**
1. Check logs: `sudo journalctl -u crypto-lake -n 50`
2. Verify Python path: `/home/Eschaton/crypto-lake/venv/bin/python --version`
3. Check environment file: `sudo cat /etc/default/crypto-lake`
4. Test manual start: `cd ~/crypto-lake && venv/bin/python main.py --mode orchestrate`

**Service crashes repeatedly:**
1. Check application logs: `cat /data/logs/qa/crypto-lake.log`
2. Verify network connectivity: `curl -I https://api.binance.com`
3. Check disk space: `df -h /data`

## Cron Jobs

### Disk Cleanup (Daily at 2 AM UTC)
```cron
0 2 * * * /home/Eschaton/crypto-lake/venv/bin/python /home/Eschaton/crypto-lake/tools/disk_cleanup.py
```

### GCS Upload (Daily at 3 AM UTC)
```cron
0 3 * * * /home/Eschaton/crypto-lake/venv/bin/python /home/Eschaton/crypto-lake/tools/gcs_uploader.py
```

### Installation
```bash
crontab -e
# Add the above lines
```

## Docker Deployment

See `Dockerfile` in project root for containerized deployment to Cloud Run.

**Note:** After PR #3 (fix/dockerfile-entrypoint) is merged, use:
```bash
docker build -t crypto-lake .
docker run -e LOG_LEVEL=WARNING -v /data:/data crypto-lake
```

## Security Considerations

1. **Environment File:** Always set restrictive permissions on `/etc/default/crypto-lake`:
   ```bash
   sudo chmod 600 /etc/default/crypto-lake
   ```

2. **Service Account:** Run as non-root user (Eschaton in this example)

3. **GCS Access:** Use service account key with minimal permissions (Storage Object Admin on specific bucket)

4. **Network:** Restrict outbound connections to Binance API endpoints only (optional firewall rules)

## Monitoring

Health metrics are written to `/data/logs/health/heartbeat.json` every 60 seconds. Monitor this file for:
- Collector status
- Raw file counts
- Parquet row counts
- Disk usage

Example monitoring script:
```bash
#!/bin/bash
HEALTH_FILE="/data/logs/health/heartbeat.json"
STATUS=$(jq -r .status "$HEALTH_FILE")

if [ "$STATUS" != "healthy" ]; then
    echo "ALERT: Crypto Lake unhealthy - $STATUS"
    # Send alert to webhook/email/Slack
fi
```

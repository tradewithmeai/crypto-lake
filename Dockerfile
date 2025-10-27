# Crypto Lake - Production Container Image
# Optimised for Google Cloud Run deployment

FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies for PostgreSQL and compilation
RUN apt-get update && apt-get install -y \
    gcc \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . /app

# Set environment variables for production
ENV PYTHONUNBUFFERED=1 \
    LOG_LEVEL=WARNING \
    PYTHONDONTWRITEBYTECODE=1

# Expose port for Cloud Run (optional, Cloud Run auto-detects)
EXPOSE 8080

# Default entrypoint: run orchestrator
# Override with CMD in Cloud Run config for different modes
ENTRYPOINT ["python", "-m", "tools.orchestrator"]

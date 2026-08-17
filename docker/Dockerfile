FROM python:3.11-slim

# Install archive tools, GPG, and system utilities
RUN apt-get update && apt-get install -y --no-install-recommends \
    gnupg \
    gzip \
    tar \
    ncompress \
    unzip \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Google Cloud Storage client library
RUN pip install --no-cache-dir google-cloud-storage google-cloud-secret-manager

# Create workspace directory
WORKDIR /workspace

# Copy Python processor script
COPY processor.py /workspace/processor.py
RUN chmod +x /workspace/processor.py

ENTRYPOINT ["python3", "/workspace/processor.py"]

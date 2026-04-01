# Multi-arch: runs native on both ARM64 (Apple Silicon) and x86_64
FROM python:3.11-slim

WORKDIR /app

# Install mssql-python Linux dependencies
# See: https://github.com/microsoft/mssql-python
RUN apt-get update && apt-get install -y \
    libltdl7 \
    libkrb5-3 \
    libgssapi-krb5-2 \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ ./src/

# Copy standalone Blitz runner script
COPY run_blitz.py ./run_blitz.py

# Run the collector
CMD ["python", "-m", "src.main"]

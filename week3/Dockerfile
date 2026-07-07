# Dockerfile
# ============================================================
# Base image — slim keeps it small (~130MB vs ~900MB for full)
FROM python:3.11-slim

# Who maintains this image — good practice
LABEL maintainer="de-learning"

# Prevents Python from writing .pyc files to disk
ENV PYTHONDONTWRITEBYTECODE=1
# Prevents Python from buffering stdout/stderr — logs appear immediately
ENV PYTHONUNBUFFERED=1

# Set working directory inside the container
WORKDIR /app

# Copy requirements first — Docker caches this layer
# If requirements don't change, Docker skips this on rebuild (faster)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Default command — runs the pipeline
CMD ["python", "pipeline_runner.py"]
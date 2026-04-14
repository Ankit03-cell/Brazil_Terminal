# Use official lightweight Python image
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install system dependencies (needed for some pandas operations)
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all project files
COPY . .

# Run ingestion to build the database during the Docker build process
# This ensures the data is ready before the app starts
RUN python ingestion.py

# Hugging Face Spaces runs on port 7860 by default
EXPOSE 7860

# Command to run the application
# We use uvicorn directly or gunicorn with uvicorn workers
CMD ["uvicorn", "backend.api:app", "--host", "0.0.0.0", "--port", "7860"]

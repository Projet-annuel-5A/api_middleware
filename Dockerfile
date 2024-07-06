# Stage 1: Build stage
FROM python:3.11.9-slim as builder

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends build-essential wget && \
    rm -rf /var/lib/apt/lists/*

# Install Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Stage 2: Final stage
FROM python:3.11.9-slim

# Copy only the necessary files from the builder stage
COPY --from=builder /usr/local /usr/local

# Set the working directory
WORKDIR /app

# Copy the project files
COPY . .

# Set the entrypoint
# ENTRYPOINT ["python3"]

# Commands to run the application
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
EXPOSE 8000
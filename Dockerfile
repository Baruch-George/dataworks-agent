# Use an official Python runtime as the base image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Install necessary system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    ffmpeg \
    libxml2-dev \
    libxslt1-dev \
    zlib1g-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Set environment variables to suppress GitPython warnings
ENV GIT_PYTHON_REFRESH=quiet

# Copy the entire project to the working directory
COPY . .

# Expose port 8000 for the FastAPI application
EXPOSE 8000

# Set environment variable for AIPROXY_TOKEN
ENV AIPROXY_TOKEN=${AIPROXY_TOKEN}

# Command to run the FastAPI app using Uvicorn
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]

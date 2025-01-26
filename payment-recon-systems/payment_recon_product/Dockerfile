# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Copy requirements and install
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY app.py ./
COPY data/ ./data/

# Expose port 5000
EXPOSE 5000

# Command to run
CMD ["python", "app.py"]

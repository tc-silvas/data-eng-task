# Use an official Python runtime as the base image
FROM python:3.11-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements and source code into the container
COPY requirements.txt requirements.txt
COPY producer.py producer.py
COPY utils.py utils.py

# Install required Python libraries
RUN pip install --no-cache-dir -r requirements.txt

# Command to run the producer script
CMD ["python", "producer.py"]

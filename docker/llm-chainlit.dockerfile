# Use the official Python image as a base image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt requirements.txt

# Install the dependencies from requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8000

COPY .env .env
COPY /code ./code
COPY /data ./data




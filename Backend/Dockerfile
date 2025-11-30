# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container at /app
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Make port 5000 available to the world outside this container
EXPOSE 5000
# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container at /app
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Make port 5000 available to the world outside this container
EXPOSE 5000

# Define environment variable
ENV FLASK_APP=api.py
ENV FLASK_ENV=production

# Create a startup script to run both processes
RUN echo '#!/bin/bash\n\
    # Start Gunicorn in the background\n\
    gunicorn --bind 0.0.0.0:5000 api:app & \n\
    \n\
    # Start the trading bot in the background\n\
    python main.py & \n\
    \n\
    # Wait for any process to exit\n\
    wait -n\n\
    \n\
    # Exit with status of process that exited first\n\
    exit $?\n\
    ' > /app/start.sh && chmod +x /app/start.sh

# Run the startup script
CMD ["/app/start.sh"]

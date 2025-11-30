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
# Start Gunicorn in the background
gunicorn --bind 0.0.0.0:5000 api:app &

# Start the trading bot in the background
python main.py &

# Wait for any process to exit
wait -n

# Exit with status of process that exited first
exit $?
' > /app/start.sh && chmod +x /app/start.sh

# Run the startup script
CMD ["/app/start.sh"]

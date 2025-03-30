# Use Python base image
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Copy application files
COPY . /app


#Upgrade pip
RUN pip install --upgrade pip


# Copy requirements and install dependencies
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Expose the Flask port (default is 8050 for Dash)
EXPOSE 8050

# Run the Dash application
CMD ["python", "dash_app.py"]
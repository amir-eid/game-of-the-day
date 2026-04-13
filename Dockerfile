FROM python:3.12-slim

# Set the working directory
WORKDIR /app

# Install git (dbt often needs git to install packages/dependencies)
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

# Copy requirements first (this makes rebuilding faster if you only change code)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your project
COPY . .

# Keep the container alive so we can run commands inside it
CMD ["python", "run_pipeline.py"]
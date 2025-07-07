FROM python:3.11.8-slim

# Set working directory in the container
WORKDIR /app

# Copy source code (everything in the repo)
COPY . .

# Install Python dependencies
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# Default script
ENTRYPOINT ["python", "AEOCFO/pipeline/ABSA.py"]

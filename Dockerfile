FROM python:3.11.8-slim

# Set working directory in the container
WORKDIR /app

# Add /app to Python path so AEOCFO local package is importable
ENV PYTHONPATH="/app"

# Copy requirements file first so it's cached separately
COPY requirements.txt .

# Install Python dependencies (cached as long as requirements.txt hasn’t changed)
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# Copy over local package AEOCFO
COPY setup.py .
COPY ./AEOCFO ./AEOCFO

# Install local package AEOCFO
RUN pip install -e .

# No ENTRYPOINT or CMD — so container starts with a shell
CMD ["/bin/bash"]
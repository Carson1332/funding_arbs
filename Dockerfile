FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy project files
COPY pyproject.toml .
COPY data/ data/
COPY research/ research/
COPY backtest/ backtest/
COPY config/ config/

# Install Python dependencies
RUN pip install --no-cache-dir -e ".[dev]"

# Create cache and results directories
RUN mkdir -p data/cache results

# Default command: run backtest
CMD ["python", "run_parameter_sweep.py"]

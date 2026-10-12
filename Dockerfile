FROM python:3.12-slim

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Install system dependencies (tar, curl)
RUN apt-get update && apt-get install -y --no-install-recommends \
    tar \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy dependency files
COPY pyproject.toml uv.lock* ./

# Install python dependencies using uv
RUN uv sync --frozen --no-cache || uv sync --no-cache

# Copy application code
COPY volmigrate/ ./volmigrate/
COPY main.py ./

ENV PATH="/app/.venv/bin:$PATH"
EXPOSE 8000

CMD ["uvicorn", "volmigrate.api.main:app", "--host", "0.0.0.0", "--port", "8000"]

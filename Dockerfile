# =============================================================================
# Stage 1: Builder — install Poetry and export pinned requirements
# =============================================================================
FROM python:3.11-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

RUN pip install --no-cache-dir "poetry>=1.7,<2"

WORKDIR /build

# Copy only dependency-related files first for layer caching
COPY pyproject.toml poetry.lock* ./

# Export pinned requirements (without dev deps) for the runtime stage
RUN poetry export --without dev --format requirements.txt --output requirements.txt || \
    poetry export --format requirements.txt --output requirements.txt

# =============================================================================
# Stage 2: Runtime — lean production image
# =============================================================================
FROM python:3.11-slim AS runtime

LABEL maintainer="cbbd-etl-team" \
      version="0.1.0" \
      description="CollegeBasketballData ETL pipeline — ingests college basketball data into an S3 lakehouse"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install runtime dependencies from exported requirements
COPY --from=builder /build/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && \
    rm requirements.txt

# Copy application source and config
COPY src/ ./src/
COPY config.yaml ./config.yaml

# Set PYTHONPATH so `python -m cbbd_etl` resolves the package
ENV PYTHONPATH=/app/src

# Non-root user for security
RUN groupadd --gid 1000 etl && \
    useradd --uid 1000 --gid etl --no-create-home etl
USER etl

# Entrypoint allows: docker run <image> incremental
ENTRYPOINT ["python", "-m", "cbbd_etl"]

# Default command — run incremental pipeline
CMD ["incremental"]

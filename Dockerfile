# Command to Build: 
# docker build --build-arg SERVICE=api|orchestrator|worker -t app:SERVICE .

ARG SERVICE=api
ARG PYTHON_VERSION=3.11

FROM python:${PYTHON_VERSION}-slim AS base
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY shared/ ./shared/

FROM base AS api
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY services/api.py .
CMD ["python", "api.py"]

FROM base AS orchestrator
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY services/orchestrator.py .
CMD ["python", "orchestrator.py"]

FROM base AS worker
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY services/worker.py .
CMD ["python", "worker.py"]

FROM ${SERVICE}

FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    pyyaml \
    redis>=5.0 \
    cryptography>=40.0

COPY aqap/ ./aqap/
COPY config.yaml ./config.yaml

ENV PYTHONPATH=/app

ENTRYPOINT ["python", "-m", "aqap"]

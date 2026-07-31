# NOTE: this image is built with the REPOSITORY ROOT as the build context
# (see docker-compose.yml: build.context: . / build.dockerfile: analytics/kafka-stream-scripts/Dockerfile),
# not analytics/kafka-stream-scripts/ itself -- unlike every other Dockerfile
# in this repo. This is deliberate: it's the only way for this container to
# also get a copy of config/schemas/api_request.avsc, the same Avro contract
# api/ and analytics/opensearch/ already depend on, without duplicating it.
FROM python:3.12-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc libssl-dev \
    && rm -rf /var/lib/apt/lists/*

COPY analytics/kafka-stream-scripts/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY analytics/kafka-stream-scripts/secondary_risk_engine.py .
COPY config/schemas/api_request.avsc /app/config/schemas/api_request.avsc

ENV PYTHONUNBUFFERED=1

EXPOSE 9500

CMD ["python", "secondary_risk_engine.py"]
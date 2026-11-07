FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY producer/pyproject.toml ./
COPY producer/src ./src
RUN pip install .

COPY schemas ./schemas

CMD ["python", "-m", "producer.main"]

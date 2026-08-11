FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY pyproject.toml requirements-dev.txt ./
COPY producer_api ./producer_api
COPY consumer ./consumer
COPY web_ui ./web_ui
COPY shared ./shared
COPY scripts ./scripts

RUN python -m pip install --upgrade pip && python -m pip install . \
    && useradd --create-home --uid 10001 agrostream \
    && chown -R agrostream:agrostream /app

COPY tests ./tests
COPY docs ./docs

USER agrostream

CMD ["python", "-m", "producer_api.main"]

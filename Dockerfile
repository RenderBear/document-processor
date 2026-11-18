FROM ghcr.io/astral-sh/uv:0.8.15 AS uv

FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH"

COPY --from=uv /uv /uvx /bin/
WORKDIR /app

COPY pyproject.toml ./
COPY src ./src
RUN uv sync --no-dev --no-editable

COPY configs ./configs
RUN useradd --create-home --uid 10001 app \
    && mkdir -p /data/artifacts \
    && chown -R app:app /data

USER app
EXPOSE 8000

CMD ["uvicorn", "doc_processing.main:app", "--host", "0.0.0.0", "--port", "8000"]

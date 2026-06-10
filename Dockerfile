FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt requirements.in pyproject.toml README.md LICENSE ./
COPY panorama.py ./
COPY panorama_elt ./panorama_elt
COPY openedx_views ./openedx_views

RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir .

WORKDIR /work

ENTRYPOINT ["panorama-elt"]

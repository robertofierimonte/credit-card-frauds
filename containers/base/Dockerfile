FROM python:3.9.7-slim-bullseye

ENV PYTHONFAULTHANDLER=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONHASHSEED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    # pip
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    # poetry
    POETRY_VERSION=1.2.2 \
    POETRY_NO_INTERACTION=1

# Install GCC
RUN apt-get update && apt-get install -y build-essential

WORKDIR /tmp

# Set the app dir
RUN mkdir -p /opt/app
WORKDIR /opt/app

# Install poetry
RUN pip install --upgrade pip && \
    pip install poetry=="${POETRY_VERSION}"

# Install dependencies
COPY pyproject.toml poetry.lock ./
RUN poetry config virtualenvs.create false \
    && poetry install --only base --no-root --no-interaction --no-ansi

# Copy the source code
COPY src /opt/app/src
